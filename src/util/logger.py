import logging

def getLogger(name):
    logger = logging.getLogger(name)
    # file = logging.FileHandler("./log/Shenzhen.log");
    # file.setLevel(logging.INFO);
    # logger.addHandler(file);
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(relativeCreated)d [%(name)s] %(levelname)s:%(message)s')
    console.setFormatter(formatter)
    
    logger.addHandler(console);
    return logger;