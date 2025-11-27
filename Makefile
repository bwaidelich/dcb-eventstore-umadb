.PHONY: build
build:
	@echo "Building Rust extension (release)"
	@if [ -f Cargo.toml ]; then \
		cargo build --release; \
	else \
		DIR=$$(git ls-files | grep -m1 Cargo.toml | xargs -r -n1 dirname); \
		if [ -n "$$DIR" ]; then \
			cd "$$DIR" && cargo build --release; \
		else \
			echo "No Cargo.toml found in repository. Update the Makefile or CI to point at the correct build directory."; exit 1; \
		fi; \
	fi
