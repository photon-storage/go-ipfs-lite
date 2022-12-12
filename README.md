# Why go-ipfs-lite?
go-ipfs-lite is a minimum IPFS node implementation in Go.
The official IPFS implementation [Kubo](https://github.com/ipfs/kubo) offers maximum customization capability by allowing developers to inject their own implementation of its building components.
However, the extensibility comes with complexity, which becomes a hurdle for developers who want quick integration.
go-ipfs-lite strips out most of the interfaces and only accept plugins for basic components such as 'Datastore' and 'Blockstore'.
The slim node becomes very easy to integrate and light-weight to operate.
