Herein lies a terrible hack to ensure that the etcd container image that we use
has CA certificates in it, because otherwise we have to encode Ubuntu-ness into
`dm cluster init`.
