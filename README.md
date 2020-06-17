# Examples

Reading from the environment
```javascript
const loggingLevel = d.environment.getText({name: 'LOGGING_LEVEL', default: 'info'});
```

This will write out to the configured logger (by default the console):

```
INFO Environment variable LOGGING_LEVEL not defined, using default [info]

// or - if the variable is found

INFO Read environment variable LOGGING_LEVEL = [debug]
```