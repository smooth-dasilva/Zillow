from Utilities.cxorcl_conn import orcl_conn_class
import cx_Oracle
import config
import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib as plt
import logging




def setup_sns(logger=logging):
    '''
    Helper function to setup seaborn defaults
    Parameters:
        logger (optional): the logger to forward logs
    Returns:
        nothing
    '''
    
    try:
        sns.set(rc={'axes.facecolor':'white', 'figure.facecolor':'white'})
        sns.despine()
    except Exception as e:
        logger.info(f'Could not setup seaborn defaults {e}')

def setup_labels(ax, title, x_label, y_label, logger=logging):
    '''
    Helper function to set labels on matplotlib axis object 
    Parameters:
        ax (matplotlib axis object): the plot to label
        title (str): the title of the plot
        x_label (str): the label for the x-axis
        y_label (str): the label for the y-axis
        logger (optional): the logger to forward logs
    Returns:
        nothing
    '''

    try:
        ax.set_title(title, size=16)
        ax.set_xlabel(x_label, color='gray')
        ax.set_ylabel(y_label, color='gray')
    except Exception as e:
        logger.info(f'Could not set axis labels {e}')

def setup_grid(ax, axis, logger=logging):
    '''
    Helper function to set the ticks and custom axis aesthetic
    Parameters:
        data (matplotlib axis object): the plot to clear grid
        axis (str): the axis to grey out
        logger (optional): the logger to forward logs
    Returns:
        nothing
    '''

    try:
        ax.tick_params(labelcolor='gray', axis=axis)
        ax.grid(color='gray', axis=axis, linestyle='dashed', linewidth=.5)
    except Exception as e:
        logger.info(f'Could not set axis grid defaults {e}')

def sort_data(data, sort_by='', asc=None, logger=logging):
    '''
    Helper function to sort data in dataframe 
    Parameters:
        data (dataframe): the data to sort
        sort_by (str): the name of the column to sort by
        asc (optional: bool | None): sort the order descending/ascending (False/True)
        logger (optional): the logger to forward logs
    Returns:
        nothing
    '''

    if(asc != None):
        try:
            data = data.sort_values(by=sort_by, ascending=asc)
        except Exception as e:
            logger.info(f'Could not sort data {e}')

    return data

def create_barplot(data, title, x_label, y_label, asc=None, logger=logging):
    '''
    Creates a matplotlib axis object for a seaborn barplot
        Note: must contain column names 'data' and 'label'
    Parameters:
        data (dataframe): the data to plot
        title (str): the title of the plot
        x_label (str): the label for the x-axis
        y_label (str): the label for the y-axis
        asc (optional: bool | None): sort the order descending/ascending (False/True)
        logger (optional): the logger to forward logs
    Returns:
        ax (matplotlib axis object): the plot
    '''

    ax = None
    
    data = sort_data(data, sort_by='data', asc=asc)

    try:
        ax = sns.barplot(x='data', y='label', data=data)
    except Exception as e:
        logger.info(f'Could not create barplot {e}')

    setup_labels(ax, title, x_label, y_label, logger)
    setup_grid(ax, 'x', logger)

    return ax

def create_stackplot(data, title, x_label, y_label, logger=logging):
    '''
    Creates a matplotlib axis object for a pandas stackplot
        Note: must contain column name 'Area'
    Parameters:
        data (dataframe): the data to plot
        title (str): the title of the plot
        x_label (str): the label for the x-axis
        y_label (str): the label for the y-axis
        logger (optional): the logger to forward logs
    Returns:
        ax (matplotlib axis object): the plot
    '''

    ax = None

    try:
        ax = data.set_index('Area').T.plot(kind='bar', stacked=True, color=sns.color_palette('Paired'))
    except Exception as e:
        logger.info(f'Could not create stackpot {e}')

    setup_labels(ax, title, x_label, y_label, logger)
    setup_grid(ax, 'y', logger)

    return ax

def create_lineplot(data, title, x_label, y_label, logger=logging):
    '''
    Creates a matplotlib axis object for a pandas lineplot
        Note: must contain column name 'Area'
    Parameters:
        data (dataframe): the data to plot
        title (str): the title of the plot
        x_label (str): the label for the x-axis
        y_label (str): the label for the y-axis
        logger (optional): the logger to forward logs
    Returns:
        ax (matplotlib axis object): the plot
    '''

    ax = None
    
    try:
        ax = data.set_index('Area').T.plot(kind='line', linewidth=1, marker='o', color=sns.color_palette('bright'))
    except Exception as e:
        logger.info(f'Could not create lineplot {e}')

    setup_labels(ax, title, x_label, y_label, logger)
    setup_grid(ax, 'y', logger)

    return ax

def save_plot(plot, destination='plot.png', logger=logging):
    '''
    Saves a matplotlib axis object as a .png a destination
    Parameters:
        plot (matplotlib axis object): the plot to save
        destination (optional: str): the name and/or path to save the file (e.g. /temp/file.png)
            -- note: default behavior creates a file 'plot.png' in the working directory
        logger (optional): the logger to forward logs
    Returns:
        nothing
    '''

    setup_sns(logger)

    try:
        image = plot.get_figure()
    except Exception as e:
        logger.info(f'Could not retrieve figure {e}')
    
    try:
        image.savefig(destination, bbox_inches='tight')
    except Exception as e:
        logger.info(f'Could not save image {e}')