package main

import (
	"github.com/cube2222/octosql/plugins"

	"github.com/cube2222/octosql-plugin-elasticsearch/pkg"
)

func main() {
	plugins.Run(pkg.Creator)
}
