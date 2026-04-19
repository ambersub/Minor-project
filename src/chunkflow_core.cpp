/*
 * chunkflow_core.cpp
 *
 * C++ core for chunkflow — parallel chunked dataset processing.
 *
 * Responsibilities:
 *   - Accept a flat list of Python string records from pybind11
 *   - Split them into fixed-size chunks
 *   - Process each chunk in parallel via OpenMP
 *   - Write all results to a single CSV file
 *   - Log progress and errors to a plain-text log file
 *
 * Build requirements:
 *   - C++17
 *   - pybind11
 *   - OpenMP   (-fopenmp)
 */

#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <functional>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _OPENMP
#  include <omp.h>
#endif

namespace py = pybind11;

// -------------------------------------------------------------------------
// CSV + row-wise math (comma-separated text rows only; quoted fields supported)
// -------------------------------------------------------------------------

static void trim_inplace(std::string& s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front())))
        s.erase(s.begin());
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
        s.pop_back();
}

/** Split one CSV line; supports double quotes and escaped "" inside quoted fields. */
std::vector<std::string> split_csv_row(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    cur.reserve(64);
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    cur += '"';
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                cur += c;
            }
        } else {
            if (c == '"') {
                in_quotes = true;
            } else if (c == ',') {
                trim_inplace(cur);
                out.push_back(std::move(cur));
                cur.clear();
            } else {
                cur += c;
            }
        }
    }
    trim_inplace(cur);
    out.push_back(std::move(cur));
    return out;
}

static bool csv_field_needs_quotes(const std::string& f) {
    for (char c : f) {
        if (c == ',' || c == '"' || c == '\r' || c == '\n')
            return true;
    }
    return false;
}

/** Join fields to one CSV line (quote fields that contain comma, quote, or newline). */
std::string join_csv_row(const std::vector<std::string>& fields) {
    std::string out;
    out.reserve(fields.size() * 16);
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i)
            out += ',';
        const std::string& f = fields[i];
        if (csv_field_needs_quotes(f)) {
            out += '"';
            for (char c : f) {
                if (c == '"')
                    out += "\"\"";
                else
                    out += c;
            }
            out += '"';
        } else {
            out += f;
        }
    }
    return out;
}

static std::optional<double> parse_double_field(const std::string& s) {
    if (s.empty())
        return std::nullopt;
    try {
        size_t idx = 0;
        double v = std::stod(s, &idx);
        while (idx < s.size() && std::isspace(static_cast<unsigned char>(s[idx])))
            ++idx;
        if (idx != s.size())
            return std::nullopt;
        return v;
    } catch (...) {
        return std::nullopt;
    }
}

enum class MathOpBinary { Add, Sub, Mul, Div };

static MathOpBinary parse_op_binary(const std::string& op) {
    std::string k = op;
    std::transform(k.begin(), k.end(), k.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (k == "add" || k == "+")
        return MathOpBinary::Add;
    if (k == "sub" || k == "subtract" || k == "-")
        return MathOpBinary::Sub;
    if (k == "mul" || k == "multiply" || k == "*")
        return MathOpBinary::Mul;
    if (k == "div" || k == "divide" || k == "/")
        return MathOpBinary::Div;
    throw std::invalid_argument("unknown operation: " + op +
        " (use add, sub, mul, div or + - * /)");
}

static double apply_binary(MathOpBinary op, double a, double b) {
    switch (op) {
        case MathOpBinary::Add: return a + b;
        case MathOpBinary::Sub: return a - b;
        case MathOpBinary::Mul: return a * b;
        case MathOpBinary::Div:
            if (b == 0.0)
                throw std::invalid_argument("division by zero");
            return a / b;
    }
    return a;
}

enum class MathOpScalar { Add, Sub, Mul, Div };

static MathOpScalar parse_op_scalar(const std::string& op) {
    std::string k = op;
    std::transform(k.begin(), k.end(), k.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (k == "add" || k == "+")
        return MathOpScalar::Add;
    if (k == "sub" || k == "subtract" || k == "-")
        return MathOpScalar::Sub;
    if (k == "mul" || k == "multiply" || k == "*")
        return MathOpScalar::Mul;
    if (k == "div" || k == "divide" || k == "/")
        return MathOpScalar::Div;
    throw std::invalid_argument("unknown operation: " + op +
        " (use add, sub, mul, div or + - * /)");
}

static double apply_scalar(MathOpScalar op, double value, double scalar) {
    switch (op) {
        case MathOpScalar::Add: return value + scalar;
        case MathOpScalar::Sub: return value - scalar;
        case MathOpScalar::Mul: return value * scalar;
        case MathOpScalar::Div:
            if (scalar == 0.0)
                throw std::invalid_argument("division by zero");
            return value / scalar;
    }
    return value;
}

// -------------------------------------------------------------------------
// Extended Math Operations (sqrt, abs, pow, floor, ceil, min, max)
// -------------------------------------------------------------------------

enum class MathOpUnary { Sqrt, Abs, Floor, Ceil };

static MathOpUnary parse_op_unary(const std::string& op) {
    std::string k = op;
    std::transform(k.begin(), k.end(), k.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (k == "sqrt")
        return MathOpUnary::Sqrt;
    if (k == "abs" || k == "absolute")
        return MathOpUnary::Abs;
    if (k == "floor")
        return MathOpUnary::Floor;
    if (k == "ceil" || k == "ceiling")
        return MathOpUnary::Ceil;
    throw std::invalid_argument("unknown unary operation: " + op +
        " (use sqrt, abs, floor, ceil)");
}

static double apply_unary(MathOpUnary op, double value) {
    switch (op) {
        case MathOpUnary::Sqrt:
            if (value < 0.0)
                throw std::invalid_argument("sqrt of negative number");
            return std::sqrt(value);
        case MathOpUnary::Abs: return std::fabs(value);
        case MathOpUnary::Floor: return std::floor(value);
        case MathOpUnary::Ceil: return std::ceil(value);
    }
    return value;
}

/** Apply unary operation to one CSV row column */
std::string apply_csv_row_math_unary(
    const std::string& row,
    const std::string& op,
    int col,
    int col_out
) {
    if (col < 0 || col_out < 0)
        throw std::invalid_argument("column indices must be non-negative");

    auto fields = split_csv_row(row);
    if (fields.size() < 1)
        throw std::invalid_argument("CSV row must have at least one column");
    if (static_cast<size_t>(col) >= fields.size())
        throw std::invalid_argument("column index out of range for CSV row");

    auto v = parse_double_field(fields[static_cast<size_t>(col)]);
    if (!v)
        throw std::invalid_argument("non-numeric value in selected column");

    double result = apply_unary(parse_op_unary(op), *v);
    if (static_cast<size_t>(col_out) > fields.size())
        throw std::invalid_argument("col_out beyond one-past-last column index");

    std::string cell = format_double_cell(result);
    if (static_cast<size_t>(col_out) == fields.size())
        fields.push_back(std::move(cell));
    else
        fields[static_cast<size_t>(col_out)] = std::move(cell);

    return join_csv_row(fields);
}

/** Apply unary operation to all rows in a list */
std::vector<std::string> apply_csv_rows_math_unary(
    const std::vector<std::string>& rows,
    const std::string& op,
    int col,
    int col_out,
    bool skip_header
) {
    std::vector<std::string> out;
    out.reserve(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        if (skip_header && i == 0) {
            if (split_csv_row(rows[i]).size() < 1)
                throw std::invalid_argument(
                    "CSV only: header row must parse to at least one column");
            out.push_back(rows[i]);
            continue;
        }
        out.push_back(apply_csv_row_math_unary(rows[i], op, col, col_out));
    }
    return out;
}

/** Apply power operation: col^exponent */
std::string apply_csv_row_math_power(
    const std::string& row,
    int col,
    double exponent,
    int col_out
) {
    if (col < 0 || col_out < 0)
        throw std::invalid_argument("column indices must be non-negative");

    auto fields = split_csv_row(row);
    if (fields.size() < 1)
        throw std::invalid_argument("CSV row must have at least one column");
    if (static_cast<size_t>(col) >= fields.size())
        throw std::invalid_argument("column index out of range for CSV row");

    auto v = parse_double_field(fields[static_cast<size_t>(col)]);
    if (!v)
        throw std::invalid_argument("non-numeric value in selected column");

    double result = std::pow(*v, exponent);
    if (static_cast<size_t>(col_out) > fields.size())
        throw std::invalid_argument("col_out beyond one-past-last column index");

    std::string cell = format_double_cell(result);
    if (static_cast<size_t>(col_out) == fields.size())
        fields.push_back(std::move(cell));
    else
        fields[static_cast<size_t>(col_out)] = std::move(cell);

    return join_csv_row(fields);
}

// -------------------------------------------------------------------------
// Delimiter-based Row Processing
// -------------------------------------------------------------------------

/** Split row with custom delimiter (supports optional quote character) */
std::vector<std::string> split_delimited_row(
    const std::string& line,
    const std::string& delimiter,
    bool use_quotes = false
) {
    if (delimiter.empty())
        throw std::invalid_argument("delimiter cannot be empty");

    std::vector<std::string> out;
    std::string cur;
    cur.reserve(64);
    bool in_quotes = false;
    char quote_char = '"';

    size_t i = 0;
    while (i < line.size()) {
        if (use_quotes && line[i] == quote_char) {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == quote_char) {
                cur += quote_char;
                i += 2;
            } else {
                in_quotes = !in_quotes;
                ++i;
            }
        } else if (!in_quotes && i + delimiter.size() <= line.size() &&
                   line.substr(i, delimiter.size()) == delimiter) {
            trim_inplace(cur);
            out.push_back(std::move(cur));
            cur.clear();
            i += delimiter.size();
        } else {
            cur += line[i];
            ++i;
        }
    }
    trim_inplace(cur);
    out.push_back(std::move(cur));
    return out;
}

/** Join fields with custom delimiter */
std::string join_delimited_row(
    const std::vector<std::string>& fields,
    const std::string& delimiter
) {
    if (delimiter.empty())
        throw std::invalid_argument("delimiter cannot be empty");

    std::string out;
    out.reserve(fields.size() * 16);
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0)
            out += delimiter;
        out += fields[i];
    }
    return out;
}

// -------------------------------------------------------------------------
// Row Filtering
// -------------------------------------------------------------------------

/** Filter rows based on a Python predicate function */
std::vector<std::string> filter_rows(
    const std::vector<std::string>& rows,
    const py::object& predicate_fn
) {
    std::vector<std::string> out;
    out.reserve(rows.size());

    for (const auto& row : rows) {
        try {
            py::gil_scoped_acquire gil;
            py::object result = predicate_fn(py::str(row));
            if (result.cast<bool>()) {
                out.push_back(row);
            }
        } catch (const py::error_already_set& e) {
            throw std::runtime_error(std::string("Filter predicate error: ") + e.what());
        }
    }
    return out;
}

/** Filter based on field value (equality check). Supports delimiters. */
std::vector<std::string> filter_rows_by_field(
    const std::vector<std::string>& rows,
    int col,
    const std::string& value,
    const std::string& delimiter = ","
) {
    std::vector<std::string> out;
    out.reserve(rows.size());

    for (const auto& row : rows) {
        auto fields = split_delimited_row(row, delimiter);
        if (static_cast<size_t>(col) < fields.size() && fields[col] == value) {
            out.push_back(row);
        }
    }
    return out;
}

/** Filter rows where numeric field is in range [min_val, max_val] */
std::vector<std::string> filter_rows_by_range(
    const std::vector<std::string>& rows,
    int col,
    double min_val,
    double max_val,
    const std::string& delimiter = ","
) {
    std::vector<std::string> out;
    out.reserve(rows.size());

    for (const auto& row : rows) {
        auto fields = split_delimited_row(row, delimiter);
        if (static_cast<size_t>(col) < fields.size()) {
            auto v = parse_double_field(fields[col]);
            if (v && *v >= min_val && *v <= max_val) {
                out.push_back(row);
            }
        }
    }
    return out;
}

static std::string format_double_cell(double v) {
    std::ostringstream oss;
    oss.precision(17);
    oss << std::defaultfloat << v;
    return oss.str();
}

/**
 * Parse *row* as CSV, read numeric columns *col_left* and *col_right* (0-based),
 * compute *op*, write result to column *col_out* (append if col_out == field count).
 */
std::string apply_csv_row_math_binary(
    const std::string& row,
    const std::string& op,
    int col_left,
    int col_right,
    int col_out
) {
    if (col_left < 0 || col_right < 0 || col_out < 0)
        throw std::invalid_argument("column indices must be non-negative");

    auto fields = split_csv_row(row);
    if (fields.size() < 2)
        throw std::invalid_argument(
            "CSV only: each row must parse to at least two comma-separated columns");
    if (static_cast<size_t>(col_left) >= fields.size() ||
        static_cast<size_t>(col_right) >= fields.size())
        throw std::invalid_argument("column index out of range for CSV row");

    auto a = parse_double_field(fields[static_cast<size_t>(col_left)]);
    auto b = parse_double_field(fields[static_cast<size_t>(col_right)]);
    if (!a || !b)
        throw std::invalid_argument("non-numeric value in selected column(s)");

    double result = apply_binary(parse_op_binary(op), *a, *b);
    if (static_cast<size_t>(col_out) > fields.size())
        throw std::invalid_argument("col_out beyond one-past-last column index");

    std::string cell = format_double_cell(result);
    if (static_cast<size_t>(col_out) == fields.size())
        fields.push_back(std::move(cell));
    else
        fields[static_cast<size_t>(col_out)] = std::move(cell);

    return join_csv_row(fields);
}

/** Same as binary, but applies (value at *col*) *op* *scalar* into *col_out*. */
std::string apply_csv_row_math_scalar(
    const std::string& row,
    const std::string& op,
    int col,
    double scalar,
    int col_out
) {
    if (col < 0 || col_out < 0)
        throw std::invalid_argument("column indices must be non-negative");

    auto fields = split_csv_row(row);
    if (fields.size() < 2)
        throw std::invalid_argument(
            "CSV only: each row must parse to at least two comma-separated columns");
    if (static_cast<size_t>(col) >= fields.size())
        throw std::invalid_argument("column index out of range for CSV row");

    auto v = parse_double_field(fields[static_cast<size_t>(col)]);
    if (!v)
        throw std::invalid_argument("non-numeric value in selected column");

    double result = apply_scalar(parse_op_scalar(op), *v, scalar);
    if (static_cast<size_t>(col_out) > fields.size())
        throw std::invalid_argument("col_out beyond one-past-last column index");

    std::string cell = format_double_cell(result);
    if (static_cast<size_t>(col_out) == fields.size())
        fields.push_back(std::move(cell));
    else
        fields[static_cast<size_t>(col_out)] = std::move(cell);

    return join_csv_row(fields);
}

/** Apply *apply_csv_row_math_binary* to every row. If *skip_header* is true, row 0 is unchanged. */
std::vector<std::string> apply_csv_rows_math_binary(
    const std::vector<std::string>& rows,
    const std::string& op,
    int col_left,
    int col_right,
    int col_out,
    bool skip_header
) {
    std::vector<std::string> out;
    out.reserve(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        if (skip_header && i == 0) {
            if (split_csv_row(rows[i]).size() < 2)
                throw std::invalid_argument(
                    "CSV only: header row must parse to at least two columns");
            out.push_back(rows[i]);
            continue;
        }
        out.push_back(apply_csv_row_math_binary(rows[i], op, col_left, col_right, col_out));
    }
    return out;
}

/** Apply *apply_csv_row_math_scalar* to every row. If *skip_header* is true, row 0 is unchanged. */
std::vector<std::string> apply_csv_rows_math_scalar(
    const std::vector<std::string>& rows,
    const std::string& op,
    int col,
    double scalar,
    int col_out,
    bool skip_header
) {
    std::vector<std::string> out;
    out.reserve(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        if (skip_header && i == 0) {
            if (split_csv_row(rows[i]).size() < 2)
                throw std::invalid_argument(
                    "CSV only: header row must parse to at least two columns");
            out.push_back(rows[i]);
            continue;
        }
        out.push_back(apply_csv_row_math_scalar(rows[i], op, col, scalar, col_out));
    }
    return out;
}

// -------------------------------------------------------------------------
// Logging
// -------------------------------------------------------------------------

class Logger {
public:
    explicit Logger(const std::string& path) : path_(path) {
        // Append to existing log so resume runs keep full history
        out_.open(path, std::ios::app);
        if (!out_.is_open())
            throw std::runtime_error("Cannot open log file: " + path);
        write("INFO", "──────────── chunkflow session started ────────────");
    }

    ~Logger() {
        write("INFO", "──────────── chunkflow session ended   ────────────");
    }

    void info (const std::string& msg) { write("INFO ", msg); }
    void warn (const std::string& msg) { write("WARN ", msg); }
    void error(const std::string& msg) { write("ERROR", msg); }

private:
    std::string   path_;
    std::ofstream out_;
    std::mutex    mtx_;

    static std::string timestamp() {
        auto now = std::chrono::system_clock::now();
        std::time_t t = std::chrono::system_clock::to_time_t(now);
        char buf[32];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
        return buf;
    }

    void write(const std::string& level, const std::string& msg) {
        std::lock_guard<std::mutex> lk(mtx_);
        out_ << "[" << timestamp() << "] [" << level << "] " << msg << "\n";
        out_.flush();
    }
};

// -------------------------------------------------------------------------
// Core processing function exposed to Python
// -------------------------------------------------------------------------

/*
 * process(records, transform_fn, output_path, log_path, chunk_size, num_threads)
 *
 * Parameters
 * ----------
 * records       : list[str]   — flat list of serialised records
 * transform_fn  : callable    — Python callable, receives one str, returns str
 * output_path   : str         — path to the CSV output file
 * log_path      : str         — path to the plain-text log file
 * chunk_size    : int         — number of records per chunk (default 500)
 * num_threads   : int         — OpenMP thread count; 0 = auto (default 0)
 *
 * Returns
 * -------
 * dict with keys: total_chunks, done, failed, skipped, elapsed_seconds
 */
py::dict process(
    const std::vector<std::string>& records,
    const py::object&               transform_fn,
    const std::string&              output_path,
    const std::string&              log_path,
    int                             chunk_size  = 500,
    int                             num_threads = 0
) {
    if (chunk_size <= 0)
        throw std::invalid_argument("chunk_size must be > 0");

    // ---- configure OpenMP ----
#ifdef _OPENMP
    if (num_threads > 0)
        omp_set_num_threads(num_threads);
    int actual_threads = omp_get_max_threads();
#else
    int actual_threads = 1;
#endif

    Logger   log(log_path);
    std::ofstream out_file(output_path);
    if (!out_file.is_open())
        throw std::runtime_error("Cannot open output file: " + output_path);

    // ---- split into chunks ----
    struct Chunk { int id; std::vector<std::string> items; };
    std::vector<Chunk> chunks;
    {
        int id = 0;
        for (size_t i = 0; i < records.size(); i += chunk_size) {
            size_t end = std::min(i + (size_t)chunk_size, records.size());
            Chunk c;
            c.id = id++;
            c.items = std::vector<std::string>(records.begin() + i,
                                               records.begin() + end);
            chunks.push_back(std::move(c));
        }
    }

    int total = (int)chunks.size();
    log.info("Records: " + std::to_string(records.size()) +
             " | Chunks: " + std::to_string(total) +
             " | Chunk size: " + std::to_string(chunk_size) +
             " | OpenMP max threads (info): " + std::to_string(actual_threads) +
             " | Python transform: sequential (safe GIL)");

    // ---- chunk processing (sequential; Python transform is not OpenMP-safe) ----
    // OpenMP + multiple worker threads calling pybind11 / Python is undefined on
    // many platforms (e.g. Windows access violations). Process one chunk at a time
    // on the thread that entered this function; GIL acquire/release stays well-defined.
    auto t_start = std::chrono::steady_clock::now();

    int done_count = 0, skip_count = 0, fail_count = 0;

    for (int ci = 0; ci < total; ++ci) {
        const Chunk& chunk = chunks[ci];

        log.info("Chunk " + std::to_string(chunk.id) +
                 " started (" + std::to_string(chunk.items.size()) + " records)"
        );

        std::vector<std::string> out_records;
        out_records.reserve(chunk.items.size());
        bool had_error = false;
        std::string error_msg;

        for (const auto& item : chunk.items) {
            try {
                // Acquire GIL, call Python transform, release GIL
                py::gil_scoped_acquire gil;
                py::object result = transform_fn(py::str(item));
                out_records.push_back(result.cast<std::string>());
            } catch (const py::error_already_set& e) {
                error_msg = std::string("Python exception: ") + e.what();
                had_error = true;
                break;
            } catch (const std::exception& e) {
                error_msg = std::string("C++ exception: ") + e.what();
                had_error = true;
                break;
            }
        }

        if (had_error) {
            log.error("Chunk " + std::to_string(chunk.id) +
                      " FAILED — " + error_msg);
            ++fail_count;
        } else {
            for (const auto& rec : out_records) {
                out_file << rec << "\n";
            }
            log.info("Chunk " + std::to_string(chunk.id) + " DONE ("
                     + std::to_string(out_records.size()) + " records written)");
            ++done_count;
        }
    }

    auto t_end  = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();

    std::string summary =
        "Finished — done=" + std::to_string(done_count) +
        " skipped=" + std::to_string(skip_count) +
        " failed="  + std::to_string(fail_count) +
        " elapsed="  + std::to_string(elapsed) + "s";
    log.info(summary);

    py::dict result;
    result["total_chunks"]    = total;
    result["done"]            = done_count;
    result["skipped"]         = skip_count;
    result["failed"]          = fail_count;
    result["elapsed_seconds"] = elapsed;
    return result;
}

// -------------------------------------------------------------------------
// pybind11 module definition
// -------------------------------------------------------------------------

PYBIND11_MODULE(chunkflow_core, m) {
    m.doc() =
        "chunkflow C++ core — CSV/delimiter chunk pipeline with extended row tools "
        "(split/join, arithmetic, filtering, and custom logic).";

    // CSV and delimiter splitting/joining
    m.def("split_csv_row", &split_csv_row, py::arg("line"),
        "Split one comma-separated CSV line (RFC4180-style quotes).");
    m.def("join_csv_row", &join_csv_row, py::arg("fields"),
        "Join fields into one CSV line; output is suitable for a .csv file.");
    m.def("split_delimited_row", &split_delimited_row,
        py::arg("line"), py::arg("delimiter"), py::arg("use_quotes") = false,
        "Split row with custom delimiter (e.g., '\\t', '|', ';').");
    m.def("join_delimited_row", &join_delimited_row,
        py::arg("fields"), py::arg("delimiter"),
        "Join fields with custom delimiter.");

    // Basic binary and scalar arithmetic
    m.def("apply_csv_row_math_binary", &apply_csv_row_math_binary,
        py::arg("row"), py::arg("operation"), py::arg("col_left"),
        py::arg("col_right"), py::arg("col_out"),
        R"doc(
CSV row in, CSV row out. *row* must be comma-separated with at least two columns.
Binary math on two numeric columns (0-based indices). *operation*: add/sub/mul/div
(or + - * /). If *col_out* equals the current field count, append a column; else replace.
        )doc");

    m.def("apply_csv_row_math_scalar", &apply_csv_row_math_scalar,
        py::arg("row"), py::arg("operation"), py::arg("col"),
        py::arg("scalar"), py::arg("col_out"),
        "CSV row in/out; >=1 columns. Scalar op on *col* (add/sub/mul/div).");

    m.def("apply_csv_rows_math_binary", &apply_csv_rows_math_binary,
        py::arg("rows"), py::arg("operation"), py::arg("col_left"),
        py::arg("col_right"), py::arg("col_out"), py::arg("skip_header") = false,
        "List of CSV lines -> list of CSV lines (same constraints as row-wise binary).");

    m.def("apply_csv_rows_math_scalar", &apply_csv_rows_math_scalar,
        py::arg("rows"), py::arg("operation"), py::arg("col"),
        py::arg("scalar"), py::arg("col_out"), py::arg("skip_header") = false,
        "List of CSV lines -> list of CSV lines (same constraints as row-wise scalar).");

    // Extended unary and advanced arithmetic
    m.def("apply_csv_row_math_unary", &apply_csv_row_math_unary,
        py::arg("row"), py::arg("operation"), py::arg("col"), py::arg("col_out"),
        "Apply unary operation (sqrt, abs, floor, ceil) to a column.");

    m.def("apply_csv_rows_math_unary", &apply_csv_rows_math_unary,
        py::arg("rows"), py::arg("operation"), py::arg("col"), py::arg("col_out"),
        py::arg("skip_header") = false,
        "Apply unary operation to all rows (skips header if flag set).");

    m.def("apply_csv_row_math_power", &apply_csv_row_math_power,
        py::arg("row"), py::arg("col"), py::arg("exponent"), py::arg("col_out"),
        "Raise a numeric column to a power: col^exponent.");

    // Filtering
    m.def("filter_rows", &filter_rows,
        py::arg("rows"), py::arg("predicate_fn"),
        "Filter rows using a Python predicate function (takes row string, returns bool).");

    m.def("filter_rows_by_field", &filter_rows_by_field,
        py::arg("rows"), py::arg("col"), py::arg("value"), py::arg("delimiter") = ",",
        "Filter rows where column value equals a string. Delimiter defaults to comma.");

    m.def("filter_rows_by_range", &filter_rows_by_range,
        py::arg("rows"), py::arg("col"), py::arg("min_val"), py::arg("max_val"),
        py::arg("delimiter") = ",",
        "Filter rows where numeric column is in range [min_val, max_val].");

    // Core chunked processing
    m.def("process", &process,
        py::arg("records"),
        py::arg("transform_fn"),
        py::arg("output_path"),
        py::arg("log_path"),
        py::arg("chunk_size")  = 500,
        py::arg("num_threads") = 0,
        R"doc(
Process *records* in parallel chunks and write results to a file.

Parameters
----------
records : list[str]
    Flat list of serialised records (strings).
transform_fn : callable[[str], str]
    Called once per record.  Must accept a str and return a str.
output_path : str
    Path to the output file (overwritten if exists).
log_path : str
    Path to the plain-text progress log (always appended to).
chunk_size : int, optional
    Records per chunk.  Default 500.
num_threads : int, optional
    OpenMP thread count.  0 = let OpenMP decide (usually = CPU cores).

Returns
-------
dict
    {"total_chunks", "done", "skipped", "failed", "elapsed_seconds"}
        )doc"
    );
}
