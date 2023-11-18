import streamlit as st
import Mongo_tasks as mt
import time
from streamlit import session_state as ss
from streamlit import runtime
from streamlit.web import cli as stcli
import sys

st.markdown("# Transfer data to MySQL")

ch_name_list = mt.getAllInfo()

if 'button_clicked' not in st.session_state:
    ss.button_clicked = False

option = st.selectbox(
    'Please select Channel Name:',
     ch_name_list)


btnSave = st.button('Save to SQL',type='primary',key = 'btn1')
if btnSave:
    ss.button_clicked = True

if ss.button_clicked:
    with st.spinner('Saving...'):
        mt.saveDetails(option)        
    ss.button_clicked = False
    st.success('Channel Details Saved in SQL Successfully')
    
    time.sleep(2)
    #st.rerun()
    
    sys.argv = ["streamlit", "run", sys.argv[0]]
    sys.exit(stcli.main())