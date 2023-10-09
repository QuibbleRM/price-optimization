## Image Score
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import seaborn as sns
import tensorflow as tf
import json
import importlib
import requests


from hashlib import md5
from PIL import ImageFile
from tensorflow.keras.layers import Dense, Flatten, Dropout, BatchNormalization
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam 
from tensorflow.keras import backend as K
from sklearn.model_selection import train_test_split
from keras.applications.mobilenet import preprocess_input
from keras.preprocessing.image import load_img, img_to_array

import abc

class INimaModel(abc.ABC):
    
    @abc.abstractclassmethod
    def earth_movers_distance():
        pass

    @abc.abstractclassmethod
    def normalize_labels():
        pass

    @abc.abstractclassmethod
    def calc_mean_score():
        pass
    
    @abc.abstractclassmethod
    def predict_rating():
        pass

class NimaModel(INimaModel):
    
    model = None
    
    def earth_movers_distance(self,y_true, y_pred):
        cdf_true = K.cumsum(y_true, axis=-1)
        cdf_pred = K.cumsum(y_pred, axis=-1)
        emd = K.sqrt(K.mean(K.square(cdf_true - cdf_pred), axis=-1))
        return K.mean(emd)

    def normalize_labels(self,labels):
        labels_np = np.array(labels)
        return labels_np / labels_np.sum()


    def calc_mean_score(self,score_dist):
        score_dist = self.normalize_labels(score_dist)
        return (score_dist*np.arange(1, 11)).sum()

    
    def __init__(self,model_path: str):
        self.model = tf.keras.models.load_model(model_path,custom_objects = {'earth_movers_distance': self.earth_movers_distance})
    
    
    def predict_rating(self,img_path):
        
        result = 0
        
        try:
            img = load_img(img_path, target_size = (224, 224))
            x = img_to_array(img)
            x = np.expand_dims(x, axis=0)
            x = preprocess_input(x)
            ImageFile.LOAD_TRUNCATED_IMAGES = True

            result = self.model.predict(x, batch_size=1, verbose=1)
            result = self.calc_mean_score(result)

        except Exception as e:
		
            print(f"An error {e} has been encountered for image {img_path}")
            result = 0

        return result