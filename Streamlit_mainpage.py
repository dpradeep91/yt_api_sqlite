import streamlit as st
import time
import YT_API as yt
from streamlit import session_state as ss
from streamlit import runtime


st.markdown("# YouTube API Data Scraping")


btnSave = None
btnCancel = None



if 'b0_cnt' not in ss:
    ss.b0_cnt = False
if 'b1_cnt' not in ss:
    ss.b1_cnt = False


def count(key):
    ss[key] = True
def count_alt(key):
    ss[key] = False

channel_id = st.text_input('Enter YouTube Channel_ID:',key='inp1')
btnFetchChannelName = st.button('Fetch Channel Name', key='btn0', on_click=count, args=('b0_cnt',))
btnFetchChannelName  = ss.b0_cnt


if btnFetchChannelName:
    
    if channel_id != '':        
        channel_name = yt.getChannelInfo(channel_id)
        if len(channel_name) == 3:
            st.write('Enter correct Channel ID')
            ss.b0_cnt = False
            ss.b1_cnt = False

        else:
            st.write('The YouTube Channel Name is:', channel_name['items'][0]['snippet']['title'])

           

            btnSave = st.button('Save',type='primary',key = 'btn1', on_click=count, args=('b1_cnt',))
            

            btnSave = ss.b1_cnt            
                      

            if btnSave:
                with st.spinner('Saving...'):
                    yt.saveChannelInfo(channel_id)
                st.success('Channel Details Saved Successfully')
                time.sleep(3)
                ss.b0_cnt = False
                ss.b1_cnt = False
                #st.rerun()
                runtime.exists()
            
            else:
                pass
            
            
    else:
        st.write('Channel_ID is empty')



