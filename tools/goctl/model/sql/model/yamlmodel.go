package model

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type (
	YamlSchemaModel struct {
		files []string
	}

	YamlDB struct {
		DB         string       `yaml:"database"`
		YamlTables []*YamlTable `yaml:"tables"`
	}

	YamlTable struct {
		Name    string        `yaml:"name"`
		Columns []*YamlColumn `yaml:"columns"`
	}

	YamlColumn struct {
		Name          string            `yaml:"name"`
		DataType      string            `yaml:"dataType"`
		ColumnType    string            `yaml:"columnType"`
		Tags          []string          `yaml:"tags"`
		IsOptional    bool              `yaml:"isOptional"`
		IsNullAble    string            `yaml:"isNullAble"`
		Indices       []YamlColumnIndex `yaml:"indices"`
		ColumnDefault any               `yaml:"columnDefault"`

		Comment         string `yaml:"comment"`
		Extra           string `yaml:"extra"`
		OrdinalPosition int    `yaml:"ordinalPosition"`
	}

	YamlColumnIndex struct {
		Type string `yaml:"type"`
		Name string `yaml:"name"`
	}
)

func NewYamlSchemaModel(files []string) *YamlSchemaModel {
	return &YamlSchemaModel{
		files: files,
	}
}

func (m *YamlSchemaModel) GetAllTables() (map[string]*Table, error) {
	matchTables := make(map[string]*Table)

	for _, f := range m.files {
		tables, err := m.Parse(f)
		if err != nil {
			return nil, err
		}

		for tableName, t := range tables {
			matchTables[tableName] = t
		}
	}

	return matchTables, nil
}

func (m *YamlSchemaModel) Parse(filename string) (map[string]*Table, error) {
	if !filepath.IsAbs(filename) {
		return nil, fmt.Errorf("%s is not a valid path", filename)
	}

	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	yamlDB := &YamlDB{}
	if err = yaml.Unmarshal(bytes, yamlDB); err != nil {
		return nil, err
	}

	matchTables := make(map[string]*Table)
	for _, t := range yamlDB.YamlTables {
		table := &Table{
			Db:          yamlDB.DB,
			Table:       t.Name,
			UniqueIndex: make(map[string][]*Column),
			NormalIndex: make(map[string][]*Column),
		}

		table.Columns = make([]*Column, 0)
		for _, field := range t.Columns {
			column := &Column{
				DbColumn: &DbColumn{
					Name:            field.Name,
					DataType:        field.DataType,
					ColumnType:      field.ColumnType,
					Extra:           field.Extra,
					Comment:         field.Comment,
					ColumnDefault:   field.ColumnDefault,
					IsNullAble:      field.IsNullAble,
					OrdinalPosition: field.OrdinalPosition,
					Tags:            field.Tags,
					IsOptional:      field.IsOptional,
				},
			}

			for _, index := range field.Indices {
				if index.Type == "primary" {
					if table.PrimaryKey != nil {
						return nil, fmt.Errorf("multiple primary key in table %s", t.Name)
					}
					table.PrimaryKey = column
				} else if index.Type == "unique" {
					table.UniqueIndex[index.Name] = append(table.UniqueIndex[index.Name], column)
				} else {
					table.NormalIndex[index.Name] = append(table.NormalIndex[index.Name], column)
				}
			}

			table.Columns = append(table.Columns, column)
		}

		matchTables[t.Name] = table
	}

	return matchTables, nil
}
