# Errors package

This package provides you with basic structures and helper functions for managing errors in web services/applications. The most common errors are handled and it is easy to use them in new or ongoing projects. The main difference with the previous version of the custom errors package is type safety and embedding errors in customized application errors.

## Conflict error

conflict error represents the state when the app tries to insert or create a resource that already exists. It can be used as a zero value, initialized with struct fields or using a helper function like `Conflict`.

Let's see first with zero value:

```go
return &errors.ConflictError{}
```

the result will look like this:

```
resource already exist
```

another example is with the `Item` field value:

```go
return &errors.ConflictError{
    Item: 123,
}
```

and error will be:
when the Item is non nil value then we can use this value for additional processing.
The Msg field takes precedence over the Item field and that value will be returned as error text.
then an error will be: `article already exists`.

There are also helper functions that will make error handling more friendly.
from the above examples you can check the new format specifier `${}` which helps you to fill the Item field in the ConflictError struct. When an Item is nonnull you can read the value using `cerr, ok := errors.AsConflict(err)` then cerr contains `Item` field value `cerr.Item.(int)`

### Extending ConflictError

using struct embedding in golang we can have some sort of inheritance. In the following example, ConflictError will be embedded into MergeConflictError.

```go
type MergeConflictError struct {
	ConflictError
	Base  string   `json:"base"`
	Head  string   `json:"head"`
	Files []string `json:"files"`
}

func (e *mergeConflictError) Error() string {
	return fmt.Sprintf("merge failed for base '%s' and head '%s'", e.Base, e.Head)
}
```

### Basic Example

```go
func insertArticle(article Article) error {
    return errors.Conflict("article ${%d} already exist", article.ID)
}

func serviceInsertArticle(article Article) error {
    err := insertArticle(article)
    if cerr, ok := errors.AsConflict(err); ok {
        log.Println(cerr.Item.(int))
    }
}

func handler(w http.ResponseWriter, r *http.Request) {
    err := insertArticle(artice)
    if err != nil {
        errors.JSONResponse(w, err)
        return
    }
}
```

## NotFound error

not found error shows an error message when the resource is not found. It can be used as a zero value, initialized with struct fields or using a helper function like `NotFound`.

Let's see first with zero value:

```go
return &errors.NotFoundError{}
```

the result will look like this:

```
resource not found
```

another example is with the `Item` field value:

```go
return &errors.NotFoundError{
    Item: 123,
}
```

and error will be:
when the Item is non nil value then we can use this value for additional processing.

The Msg field takes precedence over the Item field and that value will be returned as error text.
then an error will be: `article not found`.

There are also helper functions that will make error handling more friendly.

```go
errors.NotFound("article 123 not found")
errors.NotFound("article %d not found", 123)
errors.NotFound("article ${123} not found")
errors.NotFound("article ${%d} not found", 123)
```

from the above examples you can check the new format specifier `${}` which helps you to fill the Item field in the NotFoundError struct. When an Item is nonnull you can read the value using `cerr`, ok := errors.AsConflict(err)`then cerr contains`Item`field value`cerr.Item.(int)`

### Basic Example

```go
func getArticle(id int) error {
    return errors.NotFound("article ${%d} not found", id)
}

func getArticleService(id int) (Article, error) {
    a, err := getArticle(id)
    if nferr, ok := errors.AsNotFound(err); ok {
        log.Println(nferr.Item.(int))
    }
}

func handler(w http.ResponseWriter, r *http.Request) {
    err := getArticle(artice)
    if err != nil {
        errors.JSONResponse(w, err)
        return
    }
}
```

Other types needs to be documented, you can look source code
