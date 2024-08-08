import zarr
import xarray
import datatree
import shutil
import os

import zarr

def get_all_array_paths(group, path=''):
    """
    Recursively find all array paths in a Zarr group.
    
    Parameters:
    - group: The Zarr group to traverse.
    - path: The current path in the traversal.
    
    Returns:
    - A list of all array paths.
    """
    array_paths = []
    
    for name, item in group.items():
        current_path = f"{path}/{name}" if path else name
        if isinstance(item, zarr.core.Array):
            array_paths.append(current_path)
        elif isinstance(item, zarr.hierarchy.Group):
            # Recursively get arrays in sub-groups
            array_paths.extend(get_all_array_paths(item, current_path))
    
    return array_paths

def get_leaf_group_paths(group, path=''):
    """
    Recursively find all leaf group paths in a Zarr group.
    
    Parameters:
    - group: The Zarr group to traverse.
    - path: The current path in the traversal.
    
    Returns:
    - A list of all leaf group paths.
    """
    paths = []
    has_subgroups = False
    
    for name, item in group.items():
        if isinstance(item, zarr.hierarchy.Group):
            has_subgroups = True
            current_path = f"{path}/{name}"
            # Recursively get sub-groups
            paths.extend(get_leaf_group_paths(item, current_path))
    
    # If no sub-groups were found, this is a leaf group
    if not has_subgroups:
        paths.append(path)
    
    return paths

def get_all_subgroup_paths(group, path=''):
    """
    Recursively find all subgroup paths in a Zarr group.
    
    Parameters:
    - group: The Zarr group to traverse.
    - path: The current path in the traversal.
    
    Returns:
    - A list of all subgroup paths.
    """
    paths = []
    
    for name, item in group.items():
        if isinstance(item, zarr.hierarchy.Group):
            current_path = f"{path}/{name}"
            paths.append(current_path)
            # Recursively get sub-groups
            paths.extend(get_all_subgroup_paths(item, current_path))
    
    return paths


BUCKET_URL = 's3://janelia-cosem-datasets'
REFORMATTED_CACHE = './janelia/janelia-reformatted'

shutil.rmtree(REFORMATTED_CACHE, ignore_errors=True)

def reformat_zarr_group(original_group, new_group):
    subgroups = list(original_group.groups())
    if len(subgroups):
        return {
            gname: reformat_zarr_group(g, new_group) for gname, g in subgroups
        }
    else:
        name = original_group.name.split('/')[-1]
        arr = original_group['s0']  # in janelia's schema, s0 is the full-resolution data
        # print(dir(arr))
        print(arr.dtype, arr.chunks, arr.shape, dict(arr.attrs))
        # print(arr[0])
        # zarr.copy(
        #     arr,
        #     new_group,
        #     name=name,
        #     shallow=True,
        # )
        # new_group.attrs.update({
        #     '_ARRAY_DIMENSIONS': ['x', 'y', 'z'],  # axes are ordered 'xyz' instead of 'zyx'
        # })

def cache_reformatted(original_group, dataset_name):
    reformatted_path = f'{REFORMATTED_CACHE}/{dataset_name}'
    if not os.path.exists(reformatted_path):
        new_group = zarr.open(zarr.DirectoryStore(reformatted_path))
        reformat_zarr_group(original_group, new_group)
        # zarr.consolidate_metadata(new_group)
    return reformatted_path

dataset_urls = [
    'aic_desmosome-1/aic_desmosome-1.n5/'
]

"""
for dataset_url in dataset_urls:
    original_group = zarr.open(zarr.N5FSStore(f'{BUCKET_URL}/{dataset_url}', anon=True))
    arrs = list(original_group['/em/fibsem-uint8/'].arrays())
    vararray = arrs[5][1]
    #arrays = get_all_array_paths(original_group)
    #print(arrays)

    x = xarray.DataArray(vararray, name="s5")
    print(x)
    reformatted_path = cache_reformatted(original_group, dataset_url.replace("/", "_"))
    print(reformatted_path)
    new_group = zarr.open(reformatted_path)
    print("Printing new group")
    print(new_group)

    print("Printing new group tree")
    print(zarr.tree(new_group))
"""

import fsspec, zarr
import dask.array as da # we import dask to help us manage parallel access to the big dataset
#group = zarr.open(zarr.N5FSStore('s3://janelia-cosem-datasets/jrc_hela-2/jrc_hela-2.n5', anon=True)) # access the root of the n5 container
group = zarr.open(zarr.N5FSStore('s3://janelia-cosem-datasets/aic_desmosome-3/aic_desmosome-3.n5', anon=True)) # access the root of the n5 container
zdata = group['em/fibsem-uint8/s4'] # s0 is the the full-resolution data for this particular volume
ddata = da.from_array(zdata, chunks=zdata.chunks)
result = ddata.compute() # get the first slice of the data as a numpy array
print(result, result.shape)
