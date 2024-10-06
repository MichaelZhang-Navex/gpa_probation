Welcome to your new dbt project!

## Initial Run

```bash
mise install
mise run
```

## Regular run

### Update data
```bash
# import / refresh raw excel data
make import_excel

# run everytime excel updated
make build
```

### Calculate

```bash
# roll one student
make roll_one id=123456

# roll all
make roll_all
```
