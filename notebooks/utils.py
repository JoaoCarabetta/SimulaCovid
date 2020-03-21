import colorlover as cl
from IPython.display import HTML

from paths import THEMES_PATH
import yaml
import cufflinks as cf

def display_custom_scales():
    """
    Displays custom colorscales (HTML)
    """
    
    custom_colorscales = yaml.load(open(THEMES_PATH / 'custom_colorscales.yaml', 'r'))
    rgb_custom = dict()
    
    for scale in custom_colorscales.keys():
        rgb_custom[scale] = [cf.colors.hex_to_rgb(i) for i in custom_colorscales[scale]]
    
    display(HTML(cl.to_html(rgb_custom)))