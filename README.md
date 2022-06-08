# kdtree store
## Run Tests
Following command runs all tests and benchmarks which are defined in kd_store_test.go.
`go test`

## Benchmarks Results
### Hardware 
CPU: Intel i7-8565U (8) @ 4.600GHz

Tree Size: 500'000 Key-Values / Nodes

Average Times:

### k = 100
Store Time Tot [μs] 17205059
Get Time [μs] 6
NN Time [μs] 7078099
Scan Time [μs] 450514

### k = 10 
Store Time Tot [μs] 14520022
Get Time [μs] 6
NN Time [μs] 53663
Scan Time [μs] 292156

### k = 3 
Store Time Tot [μs] 12368149
Get Time [μs] 4
NN Time [μs] 100
Scan Time [μs] 232426
