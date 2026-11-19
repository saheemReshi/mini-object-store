package metadata

// The SQLite driver is pure Go (no cgo), so both binaries build with
// CGO_ENABLED=0 and run in minimal container images. It registers itself with
// database/sql under the name "sqlite".
import _ "modernc.org/sqlite"

const sqliteDriverName = "sqlite"
