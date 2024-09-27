import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.colors import ListedColormap
import rasterio
import matplotlib.colors as colors


def plot_raster(data, title=None, cmap="viridis", vmin=0, vmax=1000, bounds=None):

    fig, ax = plt.subplots()

    # Define the colors you want
    if cmap == "custom1":
        cmap = ListedColormap(["white", "tan", "springgreen", "darkgreen"])

        # Define a normalization from values -> colors
        bin_size = (vmax - vmin) // 4
        cboundaries = range(vmin, vmax + bin_size, bin_size)
        norm = colors.BoundaryNorm(cboundaries, len(cboundaries) - 1)

        # Add a legend for labels
        legend_labels = {"tan": "low",
                         "springgreen": "medium",
                         "darkgreen": "high"}

        patches = [Patch(color=color, label=label)
                   for color, label in legend_labels.items()]

        ax.legend(handles=patches,
                  bbox_to_anchor=(1.35, 1),
                  facecolor="white")
    else:
        ax.legend()



    chm_plot = ax.imshow(data,
                         cmap=cmap,
                         norm="log",
                         interpolation="none",
                         aspect="equal",
                         vmin=vmin,
                         vmax=vmax)

    ax.set_title(title)
    if bounds:
        ax.set_xlim(bounds[0], bounds[2])
        ax.set_ylim(bounds[1], bounds[3])
    ax.set_axis_off()
    plt.show()
