# go-circular

Package circular provides a buffer with circular semantics.

## Design

The buffer is split into chunks, the last chunk is not compressed and being written to, and previous chunks
are compressed and immutable.

The buffer keeps a pointer to the current offset being written which is always incremented, while the index
of compressed chunks keeps the initial offset of each compressed chunk and its decompressed size.

If the compressed chunk is being read, it is decompressed on the fly by the Reader.
