package sqlutil

// Scannable represents an object that can be scanned into a destination.
// It provides a Scan method that populates the destination arguments with the values from the object.
type Scannable interface {
	Scan(dest ...any) error
}

// Rows represents a database result set that can be iterated over.
// It provides methods for closing the result set, checking for errors, and scanning individual rows.
type Rows interface {
	Close() error
	Err() error
	Next() bool
	Scan(dest ...any) error
}

// ScanRows iterates over database rows and applies the provided scan function to each row.
// It handles proper resource cleanup by ensuring rows are closed after scanning is complete.
// Parameters:
//   - r: A Rows interface that provides iteration over database query results
//   - scanFunc: A function that will be called for each row to perform the scanning
//
// Returns an error if scanning fails, if there's an error during iteration, or if closing the rows fails
func ScanRows(r Rows, scanFunc func(row Scannable) error) error {
	var closeErr error
	defer func() {
		if err := r.Close(); err != nil {
			closeErr = err
		}
	}()

	var scanErr error
	for r.Next() {
		err := scanFunc(r)
		if err != nil {
			scanErr = err
			break
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	if scanErr != nil {
		return scanErr
	}

	return closeErr
}
