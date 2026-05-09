# Contract-Driven SQLite Module

This module provides a way to auto-wire SQLite database operations using a contract-driven approach, inspired by `weed/driven/rest`. It uses `github.com/glebarez/sqlite` (pure Go, no CGO) and `gorm.io/gorm`.

## Features

- **Interface-Driven**: Define your data access layer as an interface or a struct with function fields.
- **Auto-Conversion**: Methods following the pattern `Method(ctx, *Params) (*Result, error)` are automatically converted to GORM operations.
- **Reflection-Based**: Uses reflection to map struct fields and tags to SQL queries.
- **Zero CGO**: Built on top of the pure Go SQLite driver.
- **Flexible Queries**: Use struct tags for `where`, `page`, `order`, `preload`, and `update` logic.

## Usage

### 1. Define your Models and Params

```go
type User struct {
    ID   uint   `gorm:"primaryKey"`
    Name string
}

type CreateUserParams struct {
    Data *User
}

type FindUserParams struct {
    ID uint `where:"id"`
}

type ListUsersParams struct {
    Limit int `page:"limit"`
}
```

### 2. Define a Repository Struct

```go
type UserRepo struct {
    CreateUser func(ctx context.Context, p *CreateUserParams) (*User, error)
    FindUser   func(ctx context.Context, p *FindUserParams) (*User, error)
    ListUsers  func(ctx context.Context, p *ListUsersParams) (*[]User, error)
}
```

### 3. Bind and Use

```go
db := sqlite.MustOpen(sqlite.Config{
    Path: "app.db",
    AutoMigrate: []any{&User{}},
})

repo := &UserRepo{}
sqlite.Bind(db, repo)

// Now use it!
user, err := repo.CreateUser(ctx, &CreateUserParams{Data: &User{Name: "Alice"}})
```

## Struct Tags

- `where:"column_name [op]"`: Adds a WHERE clause. Operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `like`, `in`, `is null`.
- `page:"limit|offset|page"`: Handles pagination.
- `order:"clause"`: Static or dynamic ordering.
- `preload:"associations|*"`: Preloads GORM associations.
- `update:"column_name"`: Specifies fields for UPDATE operations.
- `db:"table:name"`: Overrides the table name on a field.

## Operation Inference

Operations are inferred from method/field name prefixes:
- `Create*`, `Insert*`, `Add*`, `New*` -> **Create**
- `Get*`, `Find*`, `Fetch*`, `Load*` -> **Find** (single)
- `List*`, `Search*`, `All*`, `Query*` -> **List** (multiple)
- `Update*`, `Save*`, `Modify*`, `Patch*` -> **Update**
- `Delete*`, `Remove*`, `Drop*` -> **Delete**
- `Count*` -> **Count**
- `Exec*`, `Raw*` -> **Raw SQL**
