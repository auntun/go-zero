package gen

func (g *defaultGenerator) StartFromYaml(filename string, withCache, strict bool, database string) error {
	modelList, err := g.genFromDDL(filename, withCache, strict, database)
	if err != nil {
		return err
	}

	return g.createFile(modelList)
}
