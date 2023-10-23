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
    _p_GO = 1
    _market_matrix = None
    _coeff = []
    _index_to_optimize = 0
    _mc = 1
    
    def __init__(self,p_GO=1, market_matrix=None, coeff=[-0.0062, 0.0003, 0.0672, 0.1106, 0.3239, 0.015, 0.0002, 0.011, 0.42, 0.141], index_to_optimize=0, mc=1):
        self._p_GO = p_GO
        self._market_matrix = market_matrix
        self._coeff = coeff
        self._mc =  mc
        self._index_top_optimize = index_to_optimize

    def compute_revenue(self,init_price = 0, index_to_optimize = 0):
        house_attributes = self._market_matrix
        original_price = house_attributes[index_to_optimize][0]
        house_attributes[index_to_optimize][0] = init_price    
        attr_coeff = self._coeff
        
        result = np.array(house_attributes) * np.array(attr_coeff)
        util = np.sum(result, axis=1)
        exp_util = np.exp(util)
        total_util = np.sum(exp_util)
        prop_share = exp_util[index_to_optimize] / total_util
        overall_share = prop_share * self._p_GO
        
        revenue = house_attributes[index_to_optimize][0] * (1-((1-overall_share)**self._mc))

        return -revenue  


    def optimize(self, method = 'Brent'):
        result = minimize_scalar(self.compute_revenue, args=(self._index_to_optimize,), method='Brent')
    
        return (-result.fun,result.x)


    def compute_share(self, index_to_optimize = 0):
        house_attributes = self._market_matrix
        attr_coeff = self._coeff
        result = np.array(house_attributes) * np.array(attr_coeff)
        util = np.sum(result, axis=1)
        exp_util = np.exp(util)
        total_util = np.sum(exp_util)
        normalized_util = exp_util/total_util

        return normalized_util










