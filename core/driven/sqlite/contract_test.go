package sqlite_test

import (
	"context"
	"testing"
	"time"

	driven "github.com/wdvn/kit/core/driven/sqlite"
)

// --- Domain Models ---

type User struct {
	ID        uint      `gorm:"primaryKey"`
	Name      string    `gorm:"size:100;not null"`
	Email     string    `gorm:"size:200;uniqueIndex"`
	Age       int       `gorm:"default:0"`
	Active    bool      `gorm:"default:true"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

// --- Contract Params / Results ---

// CreateUserParams defines the input for creating a user.
type CreateUserParams struct {
	Data *User // the model to create
}

func (p *CreateUserParams) Table() string { return "users" }

// FindUserParams defines the input for finding a single user.
type FindUserParams struct {
	ID    uint   `where:"id"`
	Email string `where:"email"`
}

func (p *FindUserParams) Table() string { return "users" }

// ListUsersParams defines the input for listing users.
type ListUsersParams struct {
	Active   *bool  `where:"active"`
	NameLike string `where:"name like"`
	MinAge   int    `where:"age >="`
	Limit    int    `page:"limit"`
	Offset   int    `page:"offset"`
	OrderBy  string `order:"created_at desc"`
}

func (p *ListUsersParams) Table() string { return "users" }

// UpdateUserParams defines input for updating a user.
type UpdateUserParams struct {
	ID    uint   `where:"id"`
	Name  string `update:"name"`
	Email string `update:"email"`
	Age   int    `update:"age"`
}

func (p *UpdateUserParams) Table() string { return "users" }

// DeleteUserParams defines input for deleting a user.
type DeleteUserParams struct {
	ID uint `where:"id"`
}

func (p *DeleteUserParams) Table() string { return "users" }

// CountUsersParams defines input for counting users.
type CountUsersParams struct {
	Active *bool `where:"active"`
}

func (p *CountUsersParams) Table() string { return "users" }

// CountResult holds the count result.
type CountResult struct {
	Count int64
}

// AffectedResult holds rows affected.
type AffectedResult struct {
	RowsAffected int64
}

// --- Repository using Bind ---

// UserRepo is a contract-driven repository.
// Function fields are auto-wired by Bind based on name prefix.
type UserRepo struct {
	CreateUser func(ctx context.Context, params *CreateUserParams) (*User, error)
	FindUser   func(ctx context.Context, params *FindUserParams) (*User, error)
	ListUsers  func(ctx context.Context, params *ListUsersParams) (*[]User, error)
	UpdateUser func(ctx context.Context, params *UpdateUserParams) (*AffectedResult, error)
	DeleteUser func(ctx context.Context, params *DeleteUserParams) (*AffectedResult, error)
	CountUsers func(ctx context.Context, params *CountUsersParams) (*CountResult, error)
}

func TestBindRepository(t *testing.T) {
	db, _ := driven.MustOpen(driven.Config{
		Path:        ":memory:",
		AutoMigrate: []any{&User{}},
		LogLevel:    3,
	})

	repo := &UserRepo{}
	if err := driven.Bind(db, repo); err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	ctx := context.Background()

	// --- Create ---
	user, err := repo.CreateUser(ctx, &CreateUserParams{
		Data: &User{Name: "Alice", Email: "alice@example.com", Age: 30, Active: true},
	})
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	t.Logf("Created user: %+v", user)

	if user.ID == 0 {
		t.Error("expected user ID to be set after create")
	}

	// Create another user
	_, err = repo.CreateUser(ctx, &CreateUserParams{
		Data: &User{Name: "Bob", Email: "bob@example.com", Age: 25, Active: true},
	})
	if err != nil {
		t.Fatalf("CreateUser(Bob) failed: %v", err)
	}

	// --- Find ---
	found, err := repo.FindUser(ctx, &FindUserParams{ID: user.ID})
	if err != nil {
		t.Fatalf("FindUser failed: %v", err)
	}
	if found.Name != "Alice" {
		t.Errorf("expected name Alice, got %s", found.Name)
	}
	t.Logf("Found user: %+v", found)

	// --- Find by email ---
	found2, err := repo.FindUser(ctx, &FindUserParams{Email: "bob@example.com"})
	if err != nil {
		t.Fatalf("FindUser by email failed: %v", err)
	}
	if found2.Name != "Bob" {
		t.Errorf("expected name Bob, got %s", found2.Name)
	}

	// --- List ---
	users, err := repo.ListUsers(ctx, &ListUsersParams{Limit: 10})
	if err != nil {
		t.Fatalf("ListUsers failed: %v", err)
	}
	if len(*users) != 2 {
		t.Errorf("expected 2 users, got %d", len(*users))
	}
	t.Logf("Listed %d users", len(*users))

	// --- List with filter ---
	usersFiltered, err := repo.ListUsers(ctx, &ListUsersParams{MinAge: 28})
	if err != nil {
		t.Fatalf("ListUsers filtered failed: %v", err)
	}
	if len(*usersFiltered) != 1 {
		t.Errorf("expected 1 user with age >= 28, got %d", len(*usersFiltered))
	}

	// --- Count ---
	count, err := repo.CountUsers(ctx, &CountUsersParams{})
	if err != nil {
		t.Fatalf("CountUsers failed: %v", err)
	}
	if count.Count != 2 {
		t.Errorf("expected count 2, got %d", count.Count)
	}
	t.Logf("Count: %d", count.Count)

	// --- Update ---
	updated, err := repo.UpdateUser(ctx, &UpdateUserParams{
		ID:   user.ID,
		Name: "Alice Updated",
	})
	if err != nil {
		t.Fatalf("UpdateUser failed: %v", err)
	}
	if updated.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", updated.RowsAffected)
	}

	// Verify update
	verifyUser, err := repo.FindUser(ctx, &FindUserParams{ID: user.ID})
	if err != nil {
		t.Fatalf("FindUser after update failed: %v", err)
	}
	if verifyUser.Name != "Alice Updated" {
		t.Errorf("expected updated name, got %s", verifyUser.Name)
	}

	// --- Delete ---
	deleted, err := repo.DeleteUser(ctx, &DeleteUserParams{ID: found2.ID})
	if err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}
	if deleted.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", deleted.RowsAffected)
	}

	// Verify delete
	countAfter, err := repo.CountUsers(ctx, &CountUsersParams{})
	if err != nil {
		t.Fatalf("CountUsers after delete failed: %v", err)
	}
	if countAfter.Count != 1 {
		t.Errorf("expected count 1 after delete, got %d", countAfter.Count)
	}

	// --- Find not found ---
	_, err = repo.FindUser(ctx, &FindUserParams{ID: 999})
	if err == nil {
		t.Error("expected error for non-existent user")
	}
	t.Logf("Not found error: %v", err)
}

// --- Test Handler function directly ---

func TestHandlerDirect(t *testing.T) {
	db, _ := driven.MustOpen(driven.Config{
		Path:        ":memory:",
		AutoMigrate: []any{&User{}},
		LogLevel:    3,
	})

	ctx := context.Background()

	// Use Handler directly
	createUser := driven.Handler[CreateUserParams, User](db, driven.OpCreate)
	findUser := driven.Handler[FindUserParams, User](db, driven.OpFind)

	// Create
	user, err := createUser(ctx, &CreateUserParams{
		Data: &User{Name: "Direct User", Email: "direct@example.com"},
	})
	if err != nil {
		t.Fatalf("createUser failed: %v", err)
	}
	t.Logf("Created: %+v", user)

	// Find
	found, err := findUser(ctx, &FindUserParams{Email: "direct@example.com"})
	if err != nil {
		t.Fatalf("findUser failed: %v", err)
	}
	if found.Name != "Direct User" {
		t.Errorf("expected Direct User, got %s", found.Name)
	}
}
