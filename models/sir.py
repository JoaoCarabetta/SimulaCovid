from scipy.integrate import odeint
import pandas as pd
import numpy as np

# The SIR model differential equations.
def SIR(y, t, N, beta, gamma):
    
    S, I, R = y
    dSdt = -beta * S * I / N
    dIdt = beta * S * I / N - gamma * I
    dRdt = gamma * I
    
    return dSdt, dIdt, dRdt

def entrypoint(current_state, model_parameters):
    
    args = {
        'y0': (current_state['population'] - current_state['current_infected'] - 0, # S
               current_state['current_infected'],                                   # I 
               0                                                                    # R         
              ),
        't': np.linspace(0, model_parameters['days_from_t0'], model_parameters['days_from_t0']+1),
        'args': (current_state['population'],                            # N
                 model_parameters['R0'] / model_parameters['sick_days'], # beta
                 1. / model_parameters['sick_days'],                     # gamma
                )
    } 

    result = odeint(SIR, **args)
    
    result = pd.DataFrame(result, columns=['S', 'R', 'I'])
    
    result['days'] = args['t']
    result['I2'] = result['I'] * model_parameters['i2_percentage'] / 100 # severe cases
    result['I3'] = result['I'] * model_parameters['i3_percentage'] / 100 # critical cases
    result['I1'] = result['I'] - result['I2'] - result['I3']             # mild cases
    
    return result