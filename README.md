# UMBC_Data_Localization

This project seamlessly fetches data from Voltaiq and stores it locally, offering an efficient solution for managing and processing test records and device data. Additionally, it features functionality to visualize the processed results.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Setting up a Virtual Environment](#setting-up-a-virtual-environment)
3. [Installing Dependencies](#installing-dependencies)
4. [Voltaiq Env](#voltaiq-env)
5. [Usage](#usage)
6. [License](#license)

## Prerequisites

- Python 3.7 or higher
- pip (comes with Python)
- Redis
- Voltaiq Studio token

## Setting up a Virtual Environment

Virtual environments allow you to manage project-specific dependencies, which can prevent conflicts between versions.

1. **Install `virtualenv`** (If not installed)

    ```bash
    pip install virtualenv
    ```

2. **Navigate to your project directory**:

    ```bash
    cd /path/to/your/project
    ```

3. **Create a virtual environment**:

    ```bash
    virtualenv venv
    ```

4. **Activate the virtual environment**:

    - On macOS and Linux:

        ```bash
        source venv/bin/activate
        ```

    - On Windows:

        ```bash
        .\venv\Scripts\activate
        ```

    After activation, your command prompt should show the name of the virtual environment (`venv` in this case).

## Installing Dependencies

Once the virtual environment is activated, you can install the project's dependencies.

```bash
pip install -r requirements.txt
```

Then install Redis:

**On macOS:**

    ```bash
    brew install redis
    ```
**On Linux:**

    ```bash
    sudo apt update
    sudo apt install redis-server
    ```

**On Windows:**
    Check this web: https://redis.io/docs/getting-started/installation/install-redis-on-windows/#:~:text=Redis%20is%20not%20officially%20supported,Linux%20binaries%20natively%20on%20Windows.
        

## Voltaiq Env

Add your voltaiq studio token in the first line of the .env file

## Usage

### Update local database
Use the file src/updatedb.py, in this line, you can specify the device id and start time of the test record you want to update. Or you can leave the parameter empty, it will update all the database.
```
dataManager._updatedb(device_id= 1778, start_before='2023-06-22_23-59-59',start_after='2023-06-22_00-00-00')
```
### Directory Structure
#### Folder Structure
The folder structure of voltaiq data looks like this, the tr file is the metadata, and df file is the real data:
```
voltaiq_data/
|-- directory_structure.json
|-- cell_1/
|   |-- test_start_time_1/
|       |-- tr.pickle
|       |-- df.pickle
|   |-- test_start_time_2/
|       |-- tr.pickle
|       |-- df.pickle
|   |   |-- ...
|-- cell_2/
|   |-- test_start_time_1/
|       |-- tr.pickle
|       |-- df.pickle
|   |-- test_start_time_2/
|       |-- tr.pickle
|       |-- df.pickle
|   |   |-- ...
|   |-- ...
|   |-- cell_N/
|       |-- test_start_time_1/
|       |   |-- tr.pickle
|       |   |-- df.pickle
|       |-- ...
```
#### directory_structure.json 
This file contains the useful metadata for us to locate the real data, the structure of this file looks like this: 
```
{
        "uuid": "f91ca7b0-dde6-4743-b68a-c6cbd22984ec",
        "device_id": 17154,
        "tr_name": "GMFEB23S_CELL009_RPT_6_P25C_15P0PSI_20230815_R0-01-008",
        "dev_name": "GMFEB23S_CELL009",
        "start_time": "2023-08-15_08-57-21",
        "last_dp_timestamp": 1692369188000,
        "test_folder": ".../voltaiq_data/GMFEB23S_CELL009/2023-08-15_08-57-21",
        "tags": [
            "Test Type: Reference Performance Test",
            "Procedure Version: 6",
            "arbin",
            "Temperature: 25C",
            "Run Number: R0-01-008",
            "Test Date: 20230815",
            "Pressure: 15.0 PSI"
        ]
    }, ...
```
### DataManager Usage
DataManager is a robust utility class designed to manage local data, ensuring seamless interaction with the Voltaiq Studio. It encompasses functions to fetch, delete, update, filter, and process data pertaining to test records and devices. 

##### Initialization
If have trouble with using Redis:
```python
manager = DataManager()
```
If you want to use Redis as local cache:
```python
manager = DataManager(use_redis=True)
```

##### Update Local Database
To create or update the local database with all test records and devices:

```python
manager._createdb()
manager._updatedb()
```

To update specific device data:

```python
device_id = 12345  # replace with your device_id
manager._updatedb(device_id=device_id)
```
##### Check consistency
 Check the consistency between the directory structure and local database, and repair the inconsistency
```python
manager.check_and_repair_consistency()
```
##### Filtering

Filter test records based on certain parameters:

```python
filtered_test_records = manager.filter_trs(device_id="your_device_id")
```

Similarly, for filtering dataframes:

```python
filtered_dataframes = manager.filter_dfs(tags="your_tags")
```

Or, to filter both:

```python
test_records, dataframes = manager.filter_trs_and_dfs(tr_name_substring="your_tr_name_substring")
```

##### Data Processing

To process data for a specific cell:

```python
cell_name = "your_cell_name"
cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt = manager.process_cell(cell_name)
```

### Presenter

The Presenter class is designed to manage the presentation of data to a frontend. It works in tandem with a data manager to handle data processing and querying. 

#### Initialization

To create an instance of the Presenter, you must provide a `DataManager` object:

```python
presenter = Presenter()
```

#### get_measured_data_time
```python
data = presenter.get_measured_data_time(cell_cycle_metrics, cell_data, cell_data_vdf, start_time="YYYY-MM-DD_HH-MM-SS", end_time="YYYY-MM-DD_HH-MM-SS", plot_cycles=True)
```

#### get_cycle_metrics_times
```python
metrics = presenter.get_cycle_metrics_times(cell_cycle_metrics, cell_data, cell_data_vdf, start_time="YYYY-MM-DD_HH-MM-SS", end_time="YYYY-MM-DD_HH-MM-SS")
```

#### get_cycle_metrics_AhT
```python
metrics = presenter.get_cycle_metrics_AhT(cell_cycle_metrics, cell_data, cell_data_vdf, start_time="YYYY-MM-DD_HH-MM-SS", end_time="YYYY-MM-DD_HH-MM-SS")
```

### Viewer

The Viewer class is a utility designed to visualize data from a local disk pertaining to cells, particularly their timeseries data, expansions, cycle metrics, and index metrics. This class is a comprehensive tool for researchers, engineers, and anyone interested in analyzing and visualizing cell data.

#### Example useage

Get the processed data from the Presenter, then plot it

```python
presenter = Viewer()
viewer.plot_process_cell(cell_name, cell_data) # use get_measured_data_time() method to get cell_data
```

#### Recommendations
While the default downsample value is set to 100, users can adjust this parameter for finer or coarser visualizations as required.

## License
This project is licensed under the MIT License.
