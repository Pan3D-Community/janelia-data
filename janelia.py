from paraview.util.vtkAlgorithm import *
from vtkmodules.numpy_interface import dataset_adapter as dsa
from vtkmodules.vtkCommonDataModel import vtkRectilinearGrid
from vtkmodules.util.vtkAlgorithm import VTKPythonAlgorithmBase
from vtkmodules.util import vtkConstants, numpy_support
from paraview import print_error
import traceback

try:
    import numpy as np, xarray
    import fsspec, zarr
    import dask.array as da
except ImportError as e:
    print("Error importing packages",  e)
    pass

BUCKET_URL = 's3://janelia-cosem-datasets'
REFORMATTED_CACHE = './janelia/janelia-reformatted'

def createModifiedCallback(anobject):
    import weakref
    weakref_obj = weakref.ref(anobject)
    anobject = None
    def _markmodified(*args, **kwars):
        o = weakref_obj()
        if o is not None:
            o.Modified()
    return _markmodified

@smproxy.source(name="JaneliaDataset")
@smproperty.xml("""
                <StringVectorProperty command="SetDataURL"
                      name="DataURL"
                      label="Data URL"
                      number_of_elements="1">
                    <Documentation>Amazon S3 URL to the Janelia N5 data set.</Documentation>
                </StringVectorProperty>
                """)
@smproperty.xml("""
                <StringVectorProperty command="SetGroup"
                      name="Group"
                      label="Group/Array"
                      number_of_elements="1">
                    <Documentation>The group/array name to load the data from.</Documentation>
                </StringVectorProperty>
                """)
class JaneliaDataset(VTKPythonAlgorithmBase):
    def __init__(self):
        VTKPythonAlgorithmBase.__init__(self,
            nInputPorts=0,
            nOutputPorts=1,
            outputType='vtkRectilinearGrid')
        self.url = None
        self.group = None

    def SetDataURL(self, url):
        if url != None and url != self.url:
            self.url = url
            self.Modified()

    def SetGroup(self, group):
        if group != None and group != self.group:
            self.group = group
            self.Modified()

    def RequestInformation(self, request, inInfo, outInfo):
        return super().RequestInformation(request, inInfo, outInfo)

    def RequestUpdateExtent(self, request, inInfo, outInfo):
        return super().RequestUpdateExtent(request, inInfo, outInfo)

    def RequestData(self, request, inInfo, outInfo):
        if self.url == None or self.group == None:
            print_error("URL to dataset or the group to access data no provided.")
            return 1

        try:
            print(f"Getting data from {self.url} with group {self.group}")
            group = zarr.open(zarr.N5FSStore(self.url, anon=True)) # access the root of the n5 container
            zdata = group[self.group] # s0 is the the full-resolution data for this particular volume
            ddata = da.from_array(zdata, chunks=zdata.chunks)
            result = ddata.compute() 

            extents = zdata.attrs['pixelResolution']['dimensions']
            dims    = result.shape
            
            xCoords = np.linspace(0., extents[0], dims[0])
            yCoords = np.linspace(0., extents[1], dims[1])
            zCoords = np.linspace(0., extents[2], dims[2])
 
            rgrid = self.GetOutputData(outInfo, 0)
            rgrid.SetDimensions(dims[0], dims[1], dims[2])
 
            xCoords = numpy_support.numpy_to_vtk(num_array=xCoords.ravel())
            yCoords = numpy_support.numpy_to_vtk(num_array=yCoords.ravel())
            zCoords = numpy_support.numpy_to_vtk(num_array=zCoords.ravel())
 
            rgrid.SetXCoordinates(xCoords)
            rgrid.SetYCoordinates(yCoords)
            rgrid.SetZCoordinates(zCoords)
            
            output = dsa.WrapDataObject(rgrid)
            result = np.transpose(result, (2, 1, 0))
            output.PointData.append(result.flatten(), "values")
        except Exception as e:
            print_error("Exception occured while fetching data : ", e)
            print_error(traceback.format_exc())

        return 1
