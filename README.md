# slurm-squeue-cache
A go programming excercise in the hope of providing useful programs to alleviate a busy slurm controller that handles large number of jobs.

## server
Cache squeue --format=%all output in an interval defined in the source.

## client
- Fetch output from the cache server, and process the ouput according to command options.
- Most useful squeue options are implemented, but not all.
- Use the same options as squeue. However, GO flag command line parser is used, which is not the same as squeue's.
