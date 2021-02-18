import matplotlib.pyplot as plt
import numpy as np

fig, axs = plt.subplots(3, 2)

fig.set_figheight(20)
fig.set_figwidth(15)

limits = {
    "get": ((0,0.10), (0, 250)),
    "set_yes": ((0, 1), (0, 60)),
    "set_no": ((0, 1), (0, 60)),
}

title = {
    "get" : "GET on any node",
    "set_yes" : "SET on LEADER node",
    "set_no" : "SET on FOLLOWER node",
}

for i, label in enumerate(["get", "set_yes", "set_no"]):
    axylim, ax2ylim = limits[label]
    for j, server in enumerate(["cowboy", "servlet"]):
        ax = axs[i][j]
        ax2 = ax.twinx()

        data = np.genfromtxt(f"{server}_{label}.csv")

        ax.plot(data[:,0], data[:,5], 'bo-', label="resp time")
        ax2.plot(data[:,0], data[:,6], 'ro-', label="throughput")

        ax.set_title(f"{title[label]} using {server.upper()}")

        ax.set_xlabel("Number of concurrent requests")
        ax.set_ylabel("Response time (s)")
        ax2.set_ylabel("Throughput (trans/s)")

        ax.set_ylim(axylim)
        ax2.set_ylim(ax2ylim)
        ax.set_xlim((1, 10))

        ax.legend(loc="upper left")
        ax2.legend(loc="upper right")
        ax.grid()

fig.savefig("plot.pdf", bbox_inches="tight")

# plt.show()
