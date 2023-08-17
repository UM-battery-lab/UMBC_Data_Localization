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

## Voltaiq Env

Add your voltaiq studio token in the first line of .env file

## Usage

### DataManager Usage
DataManager is a robust utility class designed to manage local data, ensuring seamless interaction with the Voltaiq Studio. It encompasses functions to fetch, delete, update, filter, and process data pertaining to test records and devices. 

##### Initialization
```python
manager = DataManager()
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
manager.update_device_data(device_id)
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
processed_data = manager.process_cell(cell_name)
```

### Presenter

The Presenter class is designed to manage the presentation of data to a frontend. It works in tandem with a data manager to handle data processing and querying. 

#### Initialization

To create an instance of the Presenter, you must provide a `DataManager` object:

```python
presenter = Presenter(dataManagerInstance)
```

#### get_measured_data_time
```python
data = presenter.get_measured_data_time("cell_name_here", start_time="YYYY-MM-DD_HH-MM-SS", end_time="YYYY-MM-DD_HH-MM-SS", plot_cycles=True)
```

#### get_cycle_metrics_times
```python
metrics = presenter.get_cycle_metrics_times("cell_name_here", start_time="YYYY-MM-DD_HH-MM-SS", end_time="YYYY-MM-DD_HH-MM-SS")
```

#### get_cycle_metrics_AhT
```python
metrics = presenter.get_cycle_metrics_AhT("cell_name_here", start_time="YYYY-MM-DD_HH-MM-SS", end_time="YYYY-MM-DD_HH-MM-SS")
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
