if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    print("Rows with zero passenger: ",data['passenger_count'].isin([0]).sum())

    print("Rows with trip distance equal to zero: ",data['trip_distance'].isin([0]).sum())

   
  
    # Create new column and convert to date
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    for col in data.columns:
        if col.endswith("ID"):
            new_col = col.replace("ID", "_id")  # Add underscore after ID
            new_col = ''.join([ i.lower() if i.isupper() else i for i in new_col])  # Convert to Snake Case
            data.rename(columns={col: new_col}, inplace=True)


    values_vendor_id = data['vendor_id'].unique()
   
    # Print the unique values
    print("Unique values in the column vendor_id:", values_vendor_id)
    
    # Specify your transformation logic here
    return  data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    
    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
