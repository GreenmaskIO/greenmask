This code was copied from go std library. The idea is to edit this code for reading record as [][]byte.
I suppose it decreases costs for allocating a new memory because here in reader
we cast str := string(r.recordBuffer) and it always allocates a bunch of memory.
I suspect migration to [][]byte significantly reduce the memory consumption 
and speed up transformation process
