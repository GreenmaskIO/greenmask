package transformers

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/internal/generators"
)

var DefaultCompanyNames = []string{
	"Apex Trading", "Apex Consulting", "Apex Ventures", "Apex Solutions", "Apex Group", "Apex Partners", "Apex Holdings", "Apex Industries", "Apex Global",
	"Apex Enterprises", "Apex Strategies", "Apex Development", "Apex Resources", "Blue Horizon", "Blue River", "Blue Oak", "Blue Sky", "Blue Harbor",
	"Blue Ridge", "Blue Mountain", "Blue Crest", "Blue Valley", "Blue Stone", "Bright Path", "Bright Horizon", "Bright Future", "Bright Harbor",
	"Bright Ventures", "Bright Crest", "Bright Ridge", "Bright Solutions", "Bright Valley", "Dynamic Ventures", "Dynamic Strategies", "Dynamic Resources",
	"Dynamic Group", "Dynamic Partners", "Dynamic Enterprises", "Dynamic Holdings", "Dynamic Innovations", "Dynamic Horizon", "Dynamic Development",
	"Epic Resources", "Epic Strategies", "Epic Development", "Epic Holdings", "Epic Ventures", "Epic Enterprises", "Epic Solutions", "Epic Partners",
	"Epic Crest", "Epic Valley", "Future Horizon", "Future Crest", "Future Strategies", "Future Ventures", "Future Resources", "Future Holdings",
	"Future Path", "Future Ridge", "Future Valley", "Future Oak", "Golden Crest", "Golden Valley", "Golden Ridge", "Golden Strategies", "Golden Horizon",
	"Golden Partners", "Golden Ventures", "Golden Holdings", "Golden Resources", "Hyper Ventures", "Hyper Solutions", "Hyper Holdings", "Hyper Crest",
	"Hyper Ridge", "Hyper Strategies", "Hyper Valley", "Hyper Horizon", "Hyper Resources", "Hyper Path", "Mega Holdings", "Mega Ventures", "Mega Strategies",
	"Mega Resources", "Mega Enterprises", "Mega Solutions", "Mega Partners", "Mega Valley", "Mega Crest", "Mega Horizon", "Omni Ventures", "Omni Holdings",
	"Omni Strategies", "Omni Path", "Omni Resources", "Omni Solutions", "Omni Enterprises", "Omni Partners", "Omni Valley", "Omni Crest", "Prime Path",
	"Prime Solutions", "Prime Ventures", "Prime Holdings", "Prime Strategies", "Prime Resources", "Prime Enterprises", "Prime Partners", "Prime Ridge",
	"Prime Valley", "Quantum Ventures", "Quantum Solutions", "Quantum Holdings", "Quantum Strategies", "Quantum Partners", "Quantum Enterprises",
	"Quantum Ridge", "Quantum Valley", "Quantum Horizon", "Rapid Holdings", "Rapid Ventures", "Rapid Strategies", "Rapid Resources", "Rapid Enterprises",
	"Rapid Solutions", "Rapid Partners", "Rapid Valley", "Rapid Horizon", "Silver Crest", "Silver Valley", "Silver Ridge", "Silver Strategies",
	"Silver Ventures", "Silver Holdings", "Silver Path", "Silver Enterprises", "Silver Solutions", "Ultra Ventures", "Ultra Strategies", "Ultra Holdings",
	"Ultra Resources", "Ultra Enterprises", "Ultra Path", "Ultra Partners", "Ultra Valley", "Ultra Horizon", "Ultra Ridge",
}

var DefaultCompanySuffixes = []string{
	"Ltd.", "Inc.", "LLC.", "LLP.", "P.C.", "Corp.",
}

type CompanyDatabase struct {
	Db              map[string][]string
	Attributes      []string
	AttributesCount int
}

func (pd *CompanyDatabase) GetRandomAttribute(attr string, randomIdx uint32) string {
	attrs := pd.Db[attr]
	return attrs[randomIdx%uint32(len(attrs))]
}

func NewCompanyDatabase(data map[string][]string) *CompanyDatabase {
	if data == nil {
		panic("data is nil")
	}

	attrsCount := 0
	attributes := make([]string, 0, len(data))

	for key, _ := range data {
		attributes = append(attributes, key)
	}

	attrsCount = max(len(attributes), attrsCount)

	slices.Sort(attributes)

	return &CompanyDatabase{
		Db:              data,
		Attributes:      attributes,
		AttributesCount: attrsCount,
	}
}

var DefaultCompanyMap = map[string][]string{
	"CompanySuffix": DefaultCompanySuffixes,
	"CompanyName":   DefaultCompanyNames,
}

var DefaultCompanyDb = NewCompanyDatabase(DefaultCompanyMap)

type RandomCompanyTransformer struct {
	byteLength int
	generator  generators.Generator
	db         *CompanyDatabase
	result     map[string]string
}

func NewRandomCompanyTransformer(companyDb map[string][]string) *RandomCompanyTransformer {

	if companyDb == nil {
		companyDb = DefaultCompanyMap
	}

	db := NewCompanyDatabase(companyDb)

	return &RandomCompanyTransformer{
		db:         db,
		result:     make(map[string]string, db.AttributesCount),
		byteLength: db.AttributesCount * 4,
	}
}

func (rpt *RandomCompanyTransformer) GetDb() *CompanyDatabase {
	return rpt.db
}

func (rpt *RandomCompanyTransformer) GetCompanyName(original []byte) (map[string]string, error) {

	resBytes, err := rpt.generator.Generate(original)
	if err != nil {
		return nil, err
	}

	startIdx := 0
	for _, attr := range rpt.db.Attributes {
		attrIdx := binary.LittleEndian.Uint32(resBytes[startIdx : startIdx+4])
		rpt.result[attr] = rpt.db.GetRandomAttribute(attr, attrIdx)
		startIdx += 4
	}

	return rpt.result, nil
}

func (rpt *RandomCompanyTransformer) GetRequiredGeneratorByteLength() int {
	return rpt.byteLength
}

func (rpt *RandomCompanyTransformer) SetGenerator(g generators.Generator) error {
	if g.Size() < rpt.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", rpt.byteLength, g.Size())
	}
	rpt.generator = g
	return nil
}
