from matplotlib.lines import lineStyles
import seaborn as sns
import matplotlib.pyplot as plt
from seaborn import kdeplot

'''Module for plotting data using seaborn and matplotlib'''



def decorator(func):
    def wrapper(self,*args,**kwargs):
        self.initialize_plot()
        plot = sns.kdeplot(*args, **kwargs)
        self.apply_customizations()

        return plot

    return wrapper


class PlotBuilder:
    def __init__(self):
        '''Initializes the PlotBuilder object with default values'''
        self.style = "whitegrid"
        self.palette = sns.color_palette('muted')
        self.figsize = (12, 8)
        self.title_size = 30
        self.grid_color = '.9'

        self.ax = None
        self.fig = None

        self.rotation = 0
        self.title = None
        self.tick_labels = None
        self.limits = {'x': None, 'y': None}
        self.x_label = None
        self.y_label = None
        self.y_hline = None

    #╔════════════════════════════════════════════════════════════════════╗
    #║                          CUSTOMIZATION                             ║
    #╚════════════════════════════════════════════════════════════════════╝
    '''Methods for customizing the plot '''

    def set_style(self, style):
        self.style = style
        return self

    def set_figsize(self, width, height):
        self.figsize = (width, height)
        return self
    
    def set_tick_labels(self,labels):
        self.tick_labels = labels
        return self

    def set_limits(self, x_lim = None, y_lim = None):
        self.limits = {'x': x_lim, 'y': y_lim}
        return self

    def set_rotation(self, degrees):
        self.rotation = degrees
        return self

    def set_title(self, title):
        self.title = title
        return self
    
    def set_x_label(self, x_label):
        self.x_label = x_label
        return self
    
    def set_y_label(self, y_label):
        self.y_label = y_label
        return self
    
    def set_horisontal_line(self,y_coords):
        self.y_hline = y_coords
        return self

    #╔════════════════════════════════════════════════════════════════════╗
    #║                       PLOT INITIALIZATION                          ║
    #╚════════════════════════════════════════════════════════════════════╝
    ''' Methods for initializing the plot '''

    def initialize_plot(self):
        sns.set_style(self.style,  {'axes.grid': True, 'grid.color': self.grid_color})
        sns.set_palette(self.palette)
        if self.fig is None or self.ax is None:
            self.fig, self.ax = plt.subplots(figsize = self.figsize)

        return self
    
    def apply_customizations(self):
        if self.title:
            self.ax.set_title(self.title, fontsize=self.title_size)

        if self.tick_labels:
            self.ax.set_xticks(self.ax.get_xticks())
            self.ax.set_xticklabels(self.tick_labels,fontsize=13)

        if self.rotation:
            self.ax.tick_params(rotation= self.rotation)

        if self.limits['x']:
            self.ax.set_xlim(self.limits['x'])

        if self.limits['y']:
            self.ax.set_ylim(self.limits['y'])

        if self.x_label:
            self.ax.set_xlabel(self.x_label,fontsize=13)

        if self.y_label:
            self.ax.set_ylabel(self.y_label,fontsize=13)

        if self.y_hline:
            for line in self.y_hline:
                self.ax.axhline(line, linestyle = "--", color = 'r')


    #╔════════════════════════════════════════════════════════════════════╗
    #║                           CREATE PLOTS                             ║
    #╚════════════════════════════════════════════════════════════════════╝

    def create_barplot(self, data, x, y, *args, **kwargs):
        self.data = data
        self.x = x
        self.y = y
        self.type = 'barplot'

        self.initialize_plot()
        sns.barplot(data=data, x=x, y=y, ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    
    def create_countplot(self, data, x, *args, **kwargs):
        self.data = data
        self.x = x
        self.type = 'countplot'

        self.initialize_plot()
        sns.countplot(data=data, x=x, ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    
    def create_boxplot(self, data, x, y, *args, **kwargs):
        self.data = data
        self.x = x
        self.y = y
        self.type = 'boxplot'

        self.initialize_plot()
        sns.boxplot(data=data, x=x, y=y, ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    
    def create_heatmap(self, data, *args, **kwargs):
        self.data = data
        self.type = 'heatmap'

        self.initialize_plot()
        sns.heatmap(data, ax=self.ax,annot=True, cmap="coolwarm", *args, **kwargs)
        self.apply_customizations()

        return self
    
    def create_lineplot(self, *args, **kwargs):
        #self.data = data
        #self.x = x
        #self.y = y
        self.type = 'lineplot'

        self.initialize_plot()
        sns.lineplot(ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    
    def create_scatterplot(self, data, x, y, *args, **kwargs):
        self.data = data
        self.x = x
        self.y = y
        self.type = 'scatterplot'

        self.initialize_plot()
        sns.scatterplot(data=data, x=x, y=y, ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    
    def create_histplot(self, data,*args, **kwargs):
        self.data = data
        #self.x = x
        self.type = 'histplot'

        self.initialize_plot()
        sns.histplot(data=data, ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    

    def create_kdeplot(self, data, *args, **kwargs):
        self.__doc__ = sns.kdeplot.__doc__
        self.data = data
        self.type = 'kde'

        self.initialize_plot()
        sns.kdeplot(data=data, ax=self.ax, *args, **kwargs)
        self.apply_customizations()

        return self
    

    #╔════════════════════════════════════════════════════════════════════╗
    #║                          TRANSIT PLOTS                             ║
    #╚════════════════════════════════════════════════════════════════════╝

    