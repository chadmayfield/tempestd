Build, push, and deploy to Kubernetes:
```
ko apply -f deploy/
```
Verify the rollout with `kubectl rollout status deployment/tempestd -n tempestd`.
