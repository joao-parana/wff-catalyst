
# wff-catalyst

```bash
git clonegit@github.com:joao-parana/wff-catalyst.git
cd wff-catalyst
mvn -Drat.ignoreErrors=true -DskipTests -Dmaven.test.skip=true \
    -Dcheckstyle.skip=true -Dscalastyle.failOnViolation=false \
    package install
```


