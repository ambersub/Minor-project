/*
 * chunkflow_core.cpp
 *
 * C++ core for chunkflow — parallel chunked dataset processing.
 *
 * Responsibilities:
 *   - Accept a flat list of Python string records from pybind11
 *   - Split them into fixed-size chunks
 *   - Process each chunk in parallel via OpenMP
 *   - Write all results to a single SQLite database
 *   - Log progress and errors to a rotating plain-text log file
 *   - Support resume: chunks already marked DONE in the DB are skipped
 *
 * Build requirements:
 *   - C++17
 *   - pybind11
 *   - SQLite3  (libsqlite3-dev)
 *   - OpenMP   (-fopenmp)
 */

#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <sqlite3.h>

#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <functional>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _OPENMP
#  include <omp.h>
#endif

namespace py = pybind11;

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
// SQLite helpers
// -------------------------------------------------------------------------

class Database {
public:
    explicit Database(const std::string& path) : path_(path) {
        if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK)
            throw std::runtime_error(std::string("Cannot open DB: ") + sqlite3_errmsg(db_));

        exec(R"(
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id   INTEGER PRIMARY KEY,
                status     TEXT    NOT NULL DEFAULT 'PENDING',
                records    INTEGER NOT NULL DEFAULT 0,
                error_msg  TEXT
            );
        )");

        exec(R"(
            CREATE TABLE IF NOT EXISTS results (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id   INTEGER NOT NULL,
                record     TEXT    NOT NULL,
                FOREIGN KEY(chunk_id) REFERENCES chunks(chunk_id)
            );
        )");

        // WAL mode: safe concurrent writes from OpenMP threads
        exec("PRAGMA journal_mode=WAL;");
        exec("PRAGMA synchronous=NORMAL;");
    }

    ~Database() {
        if (db_) sqlite3_close(db_);
    }

    // Register a chunk as PENDING (ignored if already exists → resume support)
    void register_chunk(int chunk_id, int num_records) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::string sql =
            "INSERT OR IGNORE INTO chunks(chunk_id, status, records) VALUES(" +
            std::to_string(chunk_id) + ", 'PENDING', " +
            std::to_string(num_records) + ");";
        exec_locked(sql);
    }

    // Return true when the chunk was already successfully processed
    bool is_done(int chunk_id) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::string sql =
            "SELECT status FROM chunks WHERE chunk_id=" +
            std::to_string(chunk_id) + ";";
        std::string status;
        auto cb = [](void* out, int, char** vals, char**) -> int {
            *static_cast<std::string*>(out) = vals[0] ? vals[0] : "";
            return 0;
        };
        sqlite3_exec(db_, sql.c_str(), cb, &status, nullptr);
        return status == "DONE";
    }

    // Write all processed records for a chunk and mark it DONE atomically
    void save_chunk(int chunk_id, const std::vector<std::string>& records) {
        std::lock_guard<std::mutex> lk(mtx_);
        exec_locked("BEGIN;");
        for (const auto& rec : records) {
            // Use prepared statement to safely escape content
            std::string sql =
                "INSERT INTO results(chunk_id, record) VALUES(" +
                std::to_string(chunk_id) + ", ?);";
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
            sqlite3_bind_text(stmt, 1, rec.c_str(), (int)rec.size(), SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
        exec_locked(
            "UPDATE chunks SET status='DONE', records=" +
            std::to_string(records.size()) +
            " WHERE chunk_id=" + std::to_string(chunk_id) + ";"
        );
        exec_locked("COMMIT;");
    }

    // Mark a chunk as FAILED with an error message
    void fail_chunk(int chunk_id, const std::string& error) {
        std::lock_guard<std::mutex> lk(mtx_);
        sqlite3_stmt* stmt = nullptr;
        const char* sql =
            "UPDATE chunks SET status='FAILED', error_msg=? WHERE chunk_id=?;";
        sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, error.c_str(), (int)error.size(), SQLITE_TRANSIENT);
        sqlite3_bind_int (stmt, 2, chunk_id);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    // Summary counts for the Python-side RunSummary
    std::tuple<int,int,int> counts() {
        std::lock_guard<std::mutex> lk(mtx_);
        int done = 0, failed = 0, total = 0;
        auto cb = [](void* out, int, char** v, char**) -> int {
            auto* t = static_cast<std::tuple<int,int,int>*>(out);
            std::get<0>(*t) = v[0] ? std::stoi(v[0]) : 0;
            std::get<1>(*t) = v[1] ? std::stoi(v[1]) : 0;
            std::get<2>(*t) = v[2] ? std::stoi(v[2]) : 0;
            return 0;
        };
        auto t = std::make_tuple(0,0,0);
        sqlite3_exec(db_,
            "SELECT "
            "  SUM(status='DONE'),"
            "  SUM(status='FAILED'),"
            "  COUNT(*)"
            " FROM chunks;",
            cb, &t, nullptr);
        return t;
    }

private:
    std::string path_;
    sqlite3*    db_  = nullptr;
    std::mutex  mtx_;

    void exec(const std::string& sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown error";
            sqlite3_free(err);
            throw std::runtime_error("SQLite error: " + msg);
        }
    }

    // Must be called while mtx_ is already held
    void exec_locked(const std::string& sql) {
        char* err = nullptr;
        sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err);
        if (err) sqlite3_free(err);   // errors surface via status field
    }
};

// -------------------------------------------------------------------------
// Core processing function exposed to Python
// -------------------------------------------------------------------------

/*
 * process(records, transform_fn, db_path, log_path, chunk_size, num_threads)
 *
 * Parameters
 * ----------
 * records       : list[str]   — flat list of serialised records
 * transform_fn  : callable    — Python callable, receives one str, returns str
 * db_path       : str         — path to the SQLite output file
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
    const std::string&              db_path,
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
    Database db(db_path);

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

    // Register every chunk (INSERT OR IGNORE → resume-safe)
    for (const auto& c : chunks)
        db.register_chunk(c.id, (int)c.items.size());

    // ---- chunk processing (sequential; Python transform is not OpenMP-safe) ----
    // OpenMP + multiple worker threads calling pybind11 / Python is undefined on
    // many platforms (e.g. Windows access violations). Process one chunk at a time
    // on the thread that entered this function; GIL acquire/release stays well-defined.
    auto t_start = std::chrono::steady_clock::now();

    int done_count = 0, skip_count = 0, fail_count = 0;

    for (int ci = 0; ci < total; ++ci) {
        const Chunk& chunk = chunks[ci];

        if (db.is_done(chunk.id)) {
            log.info("Chunk " + std::to_string(chunk.id) + " skipped (already DONE)");
            ++skip_count;
            continue;
        }

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
            db.fail_chunk(chunk.id, error_msg);
            log.error("Chunk " + std::to_string(chunk.id) +
                      " FAILED — " + error_msg);
            ++fail_count;
        } else {
            db.save_chunk(chunk.id, out_records);
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
    m.doc() = "chunkflow C++ core — parallel chunked processing with SQLite output";

    m.def("process", &process,
        py::arg("records"),
        py::arg("transform_fn"),
        py::arg("db_path"),
        py::arg("log_path"),
        py::arg("chunk_size")  = 500,
        py::arg("num_threads") = 0,
        R"doc(
Process *records* in parallel chunks and write results to a SQLite database.

Parameters
----------
records : list[str]
    Flat list of serialised records (strings).
transform_fn : callable[[str], str]
    Called once per record.  Must accept a str and return a str.
db_path : str
    Path to the SQLite output file (created if absent; resumed if present).
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
