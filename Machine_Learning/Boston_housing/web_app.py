import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pickle import load 

st.write(""" 
# Boston House Price Prediction App

Esta aplicación predice el precio para el dataset **Boston House Price **

- **LSTAT**: lower status of the population (percent)
- **PTRATIO**: pupil-teacher ratio by town.
- **TAX**: full-value property-tax rate per $10,000.
- **RM**: average number of rooms per dwelling.
- **NOX**: nitrogen oxides concentration (parts per 10 million).
- **INDUS**: proportion of non-retail business acres per town.
""")

st.sidebar.header('Especifica los parametros')


uploaded_file = st.sidebar.file_uploader('Upload your input CSV:',type='csv')
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
else:   
    def user_input_params():
        LSTAT = st.sidebar.slider('LSTAT', 1.70, 38.00, 12.70 )
        PTRATIO = st.sidebar.slider('PTRATIO', 12.60, 22.00, 18.50)
        TAX = st.sidebar.slider('TAX', 185.00, 715.00, 400.00)
        RM = st.sidebar.slider('RM', 3.50, 8.80, 6.30)
        NOX = st.sidebar.slider('NOX', 0.30, 0.90, 0.55)
        INDUS = st.sidebar.slider('INDUS', 0.01, 30.00, 11.00 )
        data = {'LSTA' : LSTAT,
                'PTRATIO': PTRATIO,
                'TAX': TAX, 
                'RM': RM,
                'NOX': NOX,
                'INDUS': INDUS}
        features = pd.DataFrame(data, index=[0])
        return features
    df = user_input_params()

# Panel principal
st.header('Parametros Introducidos')
st.write(df)
st.write('---')

# Load model and scaler 
model = load(open('Boston_housing/model.pkl', 'rb'))
scaler = load(open('Boston_housing/scaler.pkl', 'rb'))

X = scaler.transform(df)

prediction = model.predict(X)

st.header('Prediction of MEDV')

st.write(prediction)
