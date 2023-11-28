import numpy as np
from scipy.optimize import minimize_scalar
import time

import abc


class IPriceModel(abc.ABC):
    
    @abc.abstractclassmethod
    def compute_revenue():
        pass
    
    @abc.abstractclassmethod
    def optimize():
        pass

    @abc.abstractclassmethod
    def compute_share():
        pass

    
class PriceModel(IPriceModel):

    p_GO = 1
    market_matrix = None
    coeff = []
    index_to_optimize = 0
    mc = 1
    
    def __init__(self,p_GO = 1, market_matrix = None, coeff =  [-0.0062, 0.0003, 0.0672, 0.1106, 0.3239, -0.015, 0.0002, 0.011, 0.42, 0.141], index_to_optimize = 0, mc = 1):
        
        self.p_GO = p_GO
        self.market_matrix = market_matrix
        self.coeff = coeff
        self.mc =  mc
        self._index_top_optimize = index_to_optimize

    def compute_revenue(self,init_price = 0, index_to_optimize = 0):
        
        house_attributes = self.market_matrix
        original_price = house_attributes[index_to_optimize][0]
        house_attributes[index_to_optimize][0] = init_price    
        attr_coeff = self.coeff
        
        
        result = np.array(house_attributes) * np.array(attr_coeff)
        util = np.sum(result, axis=1)
        exp_util = np.exp(util)
        total_util = np.sum(exp_util)
        prop_share = exp_util[index_to_optimize] / total_util
        overall_share = prop_share * self.p_GO
        
        #revenue = house_attributes[index_to_optimize][0] * overall_share
        #with multiple choice
        # mc for multiple choice
        #print(self._mc)
        
        revenue = house_attributes[index_to_optimize][0] * (1-((1-overall_share)**self.mc))
        
        return -revenue  


    def optimize(self, method = 'Brent'):
        
        result = minimize_scalar(self.compute_revenue, args=(self.index_to_optimize,), method='Brent')
        
        return (-result.fun,result.x)


    def compute_share(self,index_to_optimize = 0):
        
        house_attributes = self.market_matrix
        attr_coeff = self.coeff
        result = np.array(house_attributes) * np.array(attr_coeff)
        util = np.sum(result, axis=1)
        exp_util = np.exp(util)
        total_util = np.sum(exp_util)
        normalized_util = exp_util/total_util

        return normalized_util










