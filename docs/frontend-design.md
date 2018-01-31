# frontend design

## auth

The backend auth works with HTTP basic auth.

Each RPC request to a method in [cmd/dotmesh-server/pkg/main/rpc.go](cmd/dotmesh-server/pkg/main/rpc.go) should be accompanied by the following header (in bash pseudo code):

```
Authorization: Basic $(echo "${USERNAME}:${PASSWORD}" | base64)
```

To make this simple to work with - there are wrappers which will inject these credentials for you.

#### creating a new frontend rpc handler

The code for speaking to the backend rpc methods all lives in `api`.

If we were making a request to the `ListVolumesFoo` RPC - which required a `fruit` parameter - we would define it as a function inside [api/volume.js](api/volume.js) as follows:

```javascript
const volumeFoo = (payload) => ({
  method: 'AllVolumesAndClones',
  params: {
    fruit: payload.fruit
  }
})


const VolumeApi = {
  volumeFoo,
  ...
}

export default VolumeApi
```

Then you add this definition to [api/index.js](api/index.js) and wrap it in the auth wrapper.

`loaders` is the important property here - it defines the public api handlers for the rest of the app.

```javascript
import volume from './volume'
```


