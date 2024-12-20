# Initial Tests

This folder contains the initial experimentation conducted as part of the master's thesis project. These experiments were aimed at getting familiar with the middleware backends by implementing queries based on scenarios from a previous thesis.

## Overview

The tests in this folder operate on the `ints_string.csv` file located in the `/data` folder. The scenarios focus on data manipulation tasks, including:

1. **Filtering**: Removing items that do not meet specified conditions.
2. **Dropping or Filling Invalid Items**: Handling missing or erroneous data by either dropping it or filling it with appropriate values.
3. **Group-Reduce**: Aggregating data by grouping and applying reduction operations, such as sums, averages, or other statistical measures.

### Workflow

The workflow for these tests typically involved:
1. Reading the `ints_string.csv` file.
2. Applying the specified data manipulation tasks.
3. Writing the results back to a new CSV file.

## File Description

- **`ints_string.csv`**: The dataset used for all the tests. It is located in the `/data` folder.
- **`backend_name`**: Implementation of the pipeline for each backend.

## Future Updates

This folder serves as a record of preliminary experimentation and will not reflect the final benchmarking approach. Detailed analysis and results will be available in the main thesis documentation.

## Usage

To run the tests:
1. Ensure the `ints_string.csv` file is in the `/data` folder.
2. Follow the instructions in the individual scripts to execute the queries.

---

For any questions or clarifications, feel free to reach out!