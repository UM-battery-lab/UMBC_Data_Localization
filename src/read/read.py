import pickle

def load_record(file_path):
    # Load the test record from the pickle file
    with open(file_path, "rb") as f:
        test_record = pickle.load(f)
    return test_record
