import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import rasterio
import matplotlib.colors as mcolors
from matplotlib.colors import ListedColormap, LinearSegmentedColormap, BoundaryNorm
import dask.array as da
import numpy as np

def plot_raster(data, title=None, cmap="viridis", vmin=None, vmax=None, bounds=None, value=None, stops_list=None):

    # Set the default DPI
    default_dpi = 200

    # Get the shape of the array
    # Calculate the figure size based on a 1000-pixel norm/default
    height, width = data.shape
    norm_pixels = 1000
    aspect_ratio = width / height
    figsize = (norm_pixels / default_dpi * aspect_ratio, norm_pixels / default_dpi)

    # Create the figure with the specified size and DPI
    fig, ax = plt.subplots(figsize=figsize, dpi=default_dpi)

    fig.suptitle(title if title else "Raster Plot", fontsize=10)

    # Calculate histogram data
    if isinstance(data, da.Array):
        print("\tData is a dask array")
        print(f"\tVMIN: {vmin}, VMAX: {vmax}")
        data = da.where(data != -9999, data, np.nan)  # Apply the mask using da.where
        if not vmin:
            vmin = da.nanmin(data).compute()
        if not vmax:
            vmax = da.nanmax(data).compute() + 1

    else:
        data = np.ma.masked_equal(data, -9999)
        data = data[data != -9999]
        if not vmin:
            vmin = int(data.min())
        if not vmax:
            vmax = int(data.max()) + 1

    # Ensure data is 2D
    if data.ndim != 2:
        raise ValueError(f"Invalid shape {data.shape} for image data. Expected 2D array.")

    # Calculate dynamic boundaries
    if vmax > 1:
        lower_bound = (vmin // 100) * 100
        upper_bound = ((vmax // 100) + 1) * 100
        boundaries = np.arange(lower_bound, upper_bound + 100, 100)
    elif stops_list:
        boundaries = np.array([int(round(s * 100)) for s in stops_list + [vmax]])
        data = data * 100
        mean_data = da.nanmean(data).compute()
        ax.set_title(f"Mean: {mean_data}", fontsize=6)
    else:
        if vmin in [None, np.nan, -np.inf]:
            vmin = 0
        boundaries = np.array([vmin, vmax])

    # Define the colors you want
    if cmap == "custom1":
        custom_colormap(vmax, vmin, ax)
    else:
        cmap_name = cmap if cmap else "viridis"
        cmap = plt.get_cmap(cmap_name)

        # print(f"Base cmap: {base_cmap.name}")
        print(f"\tPlot Boundaries: {boundaries}")
        norm = BoundaryNorm(boundaries, cmap.N, extend="neither")

        chm_plot = ax.imshow(data,
                             cmap=cmap,
                             norm=norm,
                             interpolation="none",
                             aspect="equal")


        if bounds:
            ax.set_xlim(bounds[0], bounds[2])
            ax.set_ylim(bounds[1], bounds[3])
        ax.set_axis_off()

        # Add colorbar
        cbar = fig.colorbar(chm_plot, ax=ax, orientation='vertical')
        cbar.set_label(value if value else "Value")
        cbar.set_ticks(boundaries) # Set the tick labels to the boundaries
        ticks = [tick for tick in boundaries] if vmax != 1 else [int(tick) for tick in boundaries]
        cbar.ax.set_yticklabels(ticks)  # Format ticks as integers

        create_histogram_subplot(fig, data, boundaries, norm, cmap, xlabel=value)

        plt.show()

def create_histogram_subplot(fig, data, boundaries, norm, cmap, xlabel):
    # Create a new axis for the histogram
    ax_hist = fig.add_axes([0.1, 0.1, 0.4, 0.2])  # Adjust the position and size as needed

    # Flatten the data and remove masked values
    if not isinstance(data, da.Array):
        data = da.from_array(data)
    data = da.where(data != -9999, data, np.nan)  # Apply the mask using da.where
    data = da.ravel(data)
    data = data[~da.isnan(data)]

    # Compute the histogram
    hist, bin_edges = da.histogram(data, bins=boundaries)
    hist = hist.compute()
    print(f"\tBoundaries: {boundaries}")
    print(f"\tHistogram: {hist}")
    print(f"\tBin Edges: {bin_edges}")
    # bin_edges = bin_edges.compute()

    # Plot histogram
    for i in range(len(hist)):
        ax_hist.bar(bin_edges[i], hist[i],
                    width=bin_edges[i+1] - bin_edges[i],
                    color=cmap(norm(bin_edges[i])),
                    align='edge')

    ax_hist.set_xlabel(xlabel, fontsize=8)
    ax_hist.set_ylabel("Count", fontsize=8)


def custom_colormap(vmax, vmin, ax):
    cmap = ListedColormap(["white", "tan", "springgreen", "darkgreen"])

    # Define a normalization from values -> colors
    bin_size = (vmax - vmin) // 4
    boundaries = range(vmin, vmax + bin_size, bin_size)
    norm = BoundaryNorm(boundaries, len(boundaries) - 1)

    # Add a legend for labels
    legend_labels = {"tan": "low",
                     "springgreen": "medium",
                     "darkgreen": "high"}

    patches = [Patch(color=color, label=label)
               for color, label in legend_labels.items()]

    ax.legend(handles=patches,
              bbox_to_anchor=(1.35, 1),
              facecolor="white")

    plt.show()

def plot_histogram(data, title=None, color=None, xlabel=None, ylabel=None):
    fig, ax = plt.subplots()

    # Calculate histogram data
    if isinstance(data, da.Array):
        data = data[data != -9999]
        start = int(data.min().compute())
        stop = int(data.max().compute()) + 1
        print(f"Start: {start}, Stop: {stop}")
        bins = da.arange(start, stop, 100)
        hist, edges = da.histogram(data, bins=bins)
        hist = hist.compute()
        edges = edges.compute()
    else:
        data = data[data != -9999]
        hist, edges = np.histogram(data.flatten(), bins=30)

    # Adjust the width of the bins
    bin_width = (edges[1] - edges[0]) * 0.9  # Adjust the width to 90% of the original
    bin_centers = 0.5 * (edges[:-1] + edges[1:])

    # Plot the histogram
    ax.bar(bin_centers, hist, width=bin_width, color=color if color else "blue")

    # Set title and labels
    ax.set_title(title if title else "Histogram")
    ax.set_xlabel(xlabel if xlabel else "Value")
    ax.set_ylabel(ylabel if ylabel else "Frequency")

    plt.show()
