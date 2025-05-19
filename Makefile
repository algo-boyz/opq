# Makefile for Odin PostgreSQL Project

# --- Variables ---

# Odin compiler
ODIN := odin

# Project name (output executable name)
TARGET := todo_app

# Source files or main package directory
# If your main.odin is in the current directory, "." is fine.
# If you have multiple .odin files or a specific package structure, adjust accordingly.
SRC := ./examples

# Odin build flags (e.g., -debug, -o:speed, -o:size, etc.)
# Add -file if your main entry point is a single file and not a directory package.
# For the example main.odin, -file is appropriate if SRC is "."
ODIN_BUILD_FLAGS := -file

# Collections (if your pq package is in a subdirectory like ./pq)
# If pq is a top-level collection known to Odin, you might not need this.
COLLECTIONS := -collection:pq=../

# Linker flags for libpq on macOS (using Homebrew)
# This dynamically finds the libpq path.
LIBPQ_LDFLAGS := -L$(shell brew --prefix libpq)/lib

# Combine all extra linker flags
EXTRA_LINKER_FLAGS := $(LIBPQ_LDFLAGS)

# --- Targets ---

.PHONY: all build run clean help

# Default target: build the project
all: build

# Build the project
build:
	@echo "Building $(TARGET)..."
	$(ODIN) build $(SRC) -out:$(TARGET) $(ODIN_BUILD_FLAGS) $(COLLECTIONS) -extra-linker-flags:"$(EXTRA_LINKER_FLAGS)"
	@echo "$(TARGET) built successfully."

# Run the project (builds first if necessary)
run: build
	@echo "Running $(TARGET)..."
	./$(TARGET)

# Clean up build artifacts
clean:
	@echo "Cleaning up..."
	rm -f $(TARGET)
	@echo "Cleaned."

# Help target to display available commands
help:
	@echo "Available targets:"
	@echo "  all        - Build the project (default)"
	@echo "  build      - Compile the project"
	@echo "  run        - Compile and run the project"
	@echo "  clean      - Remove compiled executable"
	@echo "  help       - Show this help message"

