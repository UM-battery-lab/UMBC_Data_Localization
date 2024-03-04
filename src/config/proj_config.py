# GMJULY2022_PULSE_CURRENTS = [ 3.0, 1.5, -3.0, -1.5,-0.5]
# GMFEB23_PULSE_CURRENTS = [2.0, 1.0, 0.5, -2.0, -1.0, -0.5]
# UMBL2022FEB_PULSE_CURRENTS = [2.0, 1.0, 0.5, -2.0, -1.0, -0.5]
# DEFAULT_PULSE_CURRENTS = [ 2.0, 1.0, -2.0, -1.0,-0.5]
# Qmax=3.8
# # MAX_PULSES = 11

from src.config.pulse_config import GMJULY2022_PULSE_CURRENTS, GMFEB23_PULSE_CURRENTS, DEFAULT_PULSE_CURRENTS, UMBL2022FEB_PULSE_CURRENTS, Qmax

PROJECT = {
    'Default':{
        'pulse_currents':DEFAULT_PULSE_CURRENTS,
        'nominal_capacity':3.5, #A.h
        'Qmax': 3.8,
        'I_C20': 0.177,
    },
    'GMJuly2022':{
        'pulse_currents':GMJULY2022_PULSE_CURRENTS,
        'nominal_capacity':3.5, #A.h
        'Qmax': 3.8,
        'I_C20': 0.177,
    },
        'Unknown Project':{
        'pulse_currents':GMJULY2022_PULSE_CURRENTS,
        'nominal_capacity':3.5, #A.h
        'Qmax': 3.8,
        'I_C20': 0.177,
    },
    'GMFEB23':{
        'pulse_currents':GMFEB23_PULSE_CURRENTS,
        'nominal_capacity':3.5, #A.h
        'Qmax': 3.8,
        'I_C20': 0.177,
    },
    'UMBL2022FEB':{
        'pulse_currents':UMBL2022FEB_PULSE_CURRENTS,
        'nominal_capacity':2.5, #A.h
        'Qmax': 2.8,
        'I_C20': 0.125,
    }
}