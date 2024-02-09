import pyarrow.parquet as pq
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
   
    # Specify your transformation logic here
    # Read the Parquet file into a DataFrame
    
    
   
    # Convert it to a date column
    
    data['lpep_pickup_datetime', 'lpep_dropoff_datetime'].dt.date

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
