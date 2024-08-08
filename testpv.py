# trace generated using paraview version 5.11.1
#import paraview
#paraview.compatibility.major = 5
#paraview.compatibility.minor = 11

#### import the simple module from the paraview
from paraview.simple import *
#### disable automatic camera reset on 'Show'
paraview.simple._DisableFirstRenderCameraReset()

print('loading plugin')
try:
    LoadPlugin('/home/local/KHQ/abhi.yenpure/repositories/pan3d/data-fetch/janelia.py', ns=globals())
except Exception as e:
    print("Exception ", e)

data = JaneliaDataset(registrationName='janelia')
data.UpdatePipeline()
print(data.GetOutput())
