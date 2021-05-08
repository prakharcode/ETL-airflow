import logging

def _get_logger(name):
    """get_logger(name)
    
    Single function to implemnt logging throughout the application,
    can be extended to add multiple handler to provide multiclient
    logging
    """
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logger = logging.getLogger(name)
    return logger