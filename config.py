import configparser

config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read('cfg/configs/config.cfg')
