### Problem:
- You have a vendor that delivers 2 files (`data.csv`, `headers.txt`) every day at 6AM UTC. 
- Here you are provided the files in the root path as if they were already downloaded from an external source (e.g. S3 or SFTP).
- One file contains the raw pipe-delimited data (`data.csv`) without a header row.
- The other file (`headers.txt`) contains the headers in the same order as the data in `data.csv`, but each header is a separate row. 
E.g. the first column in `data.csv` is `Order ID`, the next is `Order Date` and so on.
- Write a program that takes the data from both files, transforms into a single CSV file with both header row and data, and finally loads it into a database using the provided function.
- Use the class provided in `src/main.py`, start your code in the class's `run` function, and write as if this were production-ready code. 
- We are not using a real database here, so just pretend that the Database class's `load_file` function handles everything for us.

Note: use Python >= 3.7 to solve. No other packages or dependencies are required. The goal is to show us your coding style
and see how you would approach the problem. This should not take more than 1 hour to complete. 
