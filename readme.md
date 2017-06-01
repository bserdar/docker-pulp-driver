* What is this?

This is a replacement for Crane (https://github.com/pulp/crane)

Crane is a docker-registry proxy that sends HTTP redirects for
manifest and image layers that are stored on a CDN, so a docker pull
uses the files stored in CDN. Crane does this by using metadata
written by pulp.

The docker-pulp-driver is a docker registry storage driver that
implements the low-level filesystem functions to emulate a registry
directory structure based on the same pulp metadata files Crane
uses. Actual manifest and images are read from the CDN. Because of the
way registry internals work, the driver read the manifests dir  ectly
from CDN, but sends redirects for layer requests.       

* How to add this into Docker registry?

Docker registry does not support dynamic module loading as of this
writing (there is a PR for that). So, we have to build a custom Docker
registry to get this to work. Here are the steps:

 * Fork docker registry
 * Vendor this project in the forked copy. They use vndr, not govendor
 * In cmd/registry/main.go, import github.com/bserdar/docker-pulp-driver/pulp

That's sufficient to build. For configuration:

```
storage:
    delete:
      enabled: true
    cache:
        blobdescriptor: redis
    pulp:
        pollingdir: /var/lib/pulpmd
        pollingInterval: 60
        maxthreads: 100
    maintenance:
        uploadpurging:
            enabled: false
    redirect:
        disable: false
```

Here, the important things are:
  * storage.pulp: Tells Docker registry to use this driver
  * storage.redirect: Note the double-negative. This causes docker to check if driver requests a redirect for layer requests.
  
