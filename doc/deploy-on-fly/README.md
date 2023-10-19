# Deploy on Fly.io

The Corrosion repository on GitHub includes example files to deploy Fly Machines running Corrosion in a cluster, communicating via [Fly private networking](https://fly.io/docs/reference/private-networking/).

- [Launch a Corrosion cluster on Fly.io](./launch.md)
- [Work with cluster data on Fly.io](./play.md)
- [Connect from your local machine](./proxy-port.md)

Corrosion is designed to run on the same node as any program that uses it. On Fly.io, that means deploying from a Docker image that runs both your code and Corrosion.

It's also possible for your other Machines on the same Fly private network to read from and write to their nearest Corrosion node via [API](../api/). We don't recommend this for production use, but it's handy for occasional or development use.