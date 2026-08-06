.PHONY: all clean priority-queue-candidate.so

# The evaluator and candidate run on the same host. Let LLVM use the host's
# available atomic/vector instructions while keeping the queue implementation
# and ABI portable at the source level.
RUSTFLAGS ?= -C target-cpu=native -C codegen-units=1

all: priority-queue-candidate.so

priority-queue-candidate.so: Cargo.toml Cargo.lock src/lib.rs src/ffi.rs
	RUSTFLAGS="$(RUSTFLAGS)" cargo build --release --locked
	cp "target/release/libpriority_queue_candidate.$$(test "$$(uname -s)" = Darwin && printf dylib || printf so)" "$@"

clean:
	cargo clean
	rm -f priority-queue-candidate.so
