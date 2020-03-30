from scipy.integrate import odeint
import pandas as pd
import numpy as np

# The SIR model differential equations.
def SEIR(y, t, N, beta, gamma, sigma):

    S, E, I, R = y
    dSdt = - beta * S * I / N
    dEdt = beta * S * I / N - sigma * E
    dIdt = sigma * E - gamma * I
    dRdt = gamma * I
     
    return dSdt, dEdt, dIdt, dRdt

def entrypoint(current_state, model_parameters):
    """
        model_parameters:
            seir:
                sick_days:
                i2_percentage: Severe infection 
                i3_percentage: Critical infection 
                days_from_t0: Days of simulation
                sigma: rate of progression from the exposed to infected class
                
        current_state:
            N: Poupulation
            S: Initial Susceptible individuals
            E: Initial Exposed
            I: Initial Infected
            R: Initial Recovered individuals, who have recovered from disease and are now immune
        MODEL PARAMETERS:
            * beta:  rate at which infected individuals in class I contact Susceptibles and Infect them 
            * gamma: rate at which infected individuals in class I Recover from disease and become immune
            * sigma: rate of progression from the exposed to infected

        OBS: TODO
            Exposed is being estimated given diseases growth of 33% per day.
    """
    args = {
        'y0': (current_state['suceptible'],                                         # S
               current_state['exposed'],                                            # E
               current_state['current_infected'],                                   # I 
               current_state['recovered']                                           # R         
              ),
        't': np.linspace(0, model_parameters['days_from_t0'], model_parameters['days_from_t0']+1),
        'args': (current_state['population'],                            # N
                 model_parameters['R0'] / model_parameters['sick_days'], # beta
                 1. / model_parameters['sick_days'],                     # gamma
                 model_parameters['sigma'],                              # sigma
                )
    } 

    result = odeint(SEIR, **args)
    result = pd.DataFrame(result, columns=['S' ,'E' ,'I', 'R'])

    result['days'] = args['t']
    result['I2'] = result['I'] * model_parameters['i2_percentage'] / 100 # severe cases
    result['I3'] = result['I'] * model_parameters['i3_percentage'] / 100 # critical cases
    result['I1'] = result['I'] - result['I2'] - result['I3']             # mild cases

    return result 