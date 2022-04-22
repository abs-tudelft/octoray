from FWI.grid2D import grid2D
from FWI.sources import Sources
from FWI.receivers import Receivers
from FWI.frequenciesGroup import frequenciesGroup
from FWI.finiteDifferenceForwardModel import FiniteDifferenceForwardModel
from FWI.conjugateGradientInversion import ConjugateGradientInversion



class FWI():
    def __init__(self,A,B,C,D,E,F,dotprod,update,config,resolution,gridsize,acceleration=True):
        self.grid = grid2D([config["reservoirTopLeft"]["x"], config["reservoirTopLeft"]["z"]],
                      [config["reservoirBottomRight"]["x"], config["reservoirBottomRight"]["z"]],
                      [config["ngrid"]["x"], config["ngrid"]["z"]])
        self.source = Sources([config["sourcesTopLeft"]["x"], config["sourcesTopLeft"]["z"]],
                         [config["sourcesBottomRight"]["x"], config["sourcesBottomRight"]["z"]],
                         config["nSources"])
        self.receiver = Receivers([config["receiversTopLeft"]["x"], config["receiversTopLeft"]["z"]],
                             [config["receiversBottomRight"]["x"], config["receiversBottomRight"]["z"]],
                             config["nReceivers"])
        self.freq = frequenciesGroup(config["Freq"], config["c_0"])
        self.config = config

        self.model = FiniteDifferenceForwardModel(self.grid, self.source, self.receiver, self.freq, None, acceleration, gridsize,resolution, dotprod, update, A,B,C)

        self.inverse = ConjugateGradientInversion(None, self.model, config,D,E,F)
        
    def pre_process(self,grid_data):
        self.referencePressureData = self.model.calculatePressureField(grid_data) 
    
    def reconstruct(self):
        self.chi = self.inverse.reconstruct(self.referencePressureData, self.config).data
        return self.chi

 
        