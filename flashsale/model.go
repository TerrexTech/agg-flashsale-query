package flashsale

import (
	"encoding/json"

	util "github.com/TerrexTech/go-commonutils/commonutil"

	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

const AggregateID int8 = 7

// Flashsale defines the Flashsale Aggregate.
type Flashsale struct {
	ID          objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	FlashID     uuuid.UUID        `bson:"flashID,omitempty" json:"flashID,omitempty"`
	ItemID      uuuid.UUID        `bson:"itemID,omitempty" json:"itemID,omitempty"`
	UPC         int64             `bson:"upc,omitempty" json:"upc,omitempty"`
	SKU         string            `bson:"sku,omitempty" json:"sku,omitempty"`
	Name        string            `bson:"name,omitempty" json:"name,omitempty"`
	Origin      string            `bson:"origin,omitempty" json:"origin,omitempty"`
	DeviceID    uuuid.UUID        `bson:"deviceID,omitempty" json:"deviceID,omitempty"`
	Price       float64           `bson:"price,omitempty" json:"price,omitempty"`
	SalePrice   float64           `bson:"salePrice,omitempty" json:"salePrice,omitempty"`
	Timestamp   int64             `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
	Ethylene    float64           `bson:"ethylene,omitempty" json:"ethylene,omitempty"`
	Status      string            `bson:"status,omitempty" json:"status,omitempty"`
	TotalWeight float64           `bson:"totalWeight,omitempty" json:"totalWeight,omitempty"`
	SoldWeight  float64           `bson:"soldWeight,omitempty" json:"soldWeight,omitempty"`
	Lot         string            `bson:"lot,omitempty" json:"lot,omitempty"`
}

// marshalFlashsale is simplified version of Flashsale, for convenience
// in Marshalling and Unmarshalling operations.
type marshalFlashsale struct {
	ID          objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	FlashID     string            `bson:"flashID,omitempty" json:"flashID,omitempty"`
	ItemID      string            `bson:"itemID,omitempty" json:"itemID,omitempty"`
	UPC         int64             `bson:"upc,omitempty" json:"upc,omitempty"`
	SKU         string            `bson:"sku,omitempty" json:"sku,omitempty"`
	Name        string            `bson:"name,omitempty" json:"name,omitempty"`
	Origin      string            `bson:"origin,omitempty" json:"origin,omitempty"`
	DeviceID    string            `bson:"deviceID,omitempty" json:"deviceID,omitempty"`
	Price       float64           `bson:"price,omitempty" json:"price,omitempty"`
	SalePrice   float64           `bson:"salePrice,omitempty" json:"salePrice,omitempty"`
	Timestamp   int64             `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
	Ethylene    float64           `bson:"ethylene,omitempty" json:"ethylene,omitempty"`
	Status      string            `bson:"status,omitempty" json:"status,omitempty"`
	TotalWeight float64           `bson:"totalWeight,omitempty" json:"totalWeight,omitempty"`
	SoldWeight  float64           `bson:"soldWeight,omitempty" json:"soldWeight,omitempty"`
	Lot         string            `bson:"lot,omitempty" json:"lot,omitempty"`
}

// MarshalBSON returns bytes of BSON-type.
func (i Flashsale) MarshalBSON() ([]byte, error) {
	in := &marshalFlashsale{
		ID:          i.ID,
		FlashID:     i.FlashID.String(),
		ItemID:      i.ItemID.String(),
		DeviceID:    i.DeviceID.String(),
		UPC:         i.UPC,
		SKU:         i.SKU,
		Name:        i.Name,
		Origin:      i.Origin,
		Price:       i.Price,
		SalePrice:   i.SalePrice,
		Timestamp:   i.Timestamp,
		Ethylene:    i.Ethylene,
		Status:      i.Status,
		SoldWeight:  i.SoldWeight,
		TotalWeight: i.TotalWeight,
		Lot:         i.Lot,
	}

	return bson.Marshal(in)
}

// MarshalJSON returns bytes of JSON-type.
func (i *Flashsale) MarshalJSON() ([]byte, error) {
	in := map[string]interface{}{
		"flashID":     i.FlashID.String(),
		"itemID":      i.ItemID.String(),
		"deviceID":    i.DeviceID.String(),
		"upc":         i.UPC,
		"sku":         i.SKU,
		"name":        i.Name,
		"origin":      i.Origin,
		"price":       i.Price,
		"salePrice":   i.SalePrice,
		"timestamp":   i.Timestamp,
		"ethylene":    i.Ethylene,
		"status":      i.Status,
		"soldWeight":  i.SoldWeight,
		"totalWeight": i.TotalWeight,
		"lot":         i.Lot,
	}

	if i.ID != objectid.NilObjectID {
		in["_id"] = i.ID.Hex()
	}
	return json.Marshal(in)
}

// UnmarshalBSON returns BSON-type from bytes.
func (i *Flashsale) UnmarshalBSON(in []byte) error {
	m := make(map[string]interface{})
	err := bson.Unmarshal(in, m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	err = i.unmarshalFromMap(m)
	return err
}

// UnmarshalJSON returns JSON-type from bytes.
func (i *Flashsale) UnmarshalJSON(in []byte) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(in, &m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	err = i.unmarshalFromMap(m)
	return err
}

// unmarshalFromMap unmarshals Map into Flashsale.
func (i *Flashsale) unmarshalFromMap(m map[string]interface{}) error {
	var err error
	var assertOK bool

	// Hoping to discover a better way to do this someday
	if m["_id"] != nil {
		i.ID, assertOK = m["_id"].(objectid.ObjectID)
		if !assertOK {
			i.ID, err = objectid.FromHex(m["_id"].(string))
			if err != nil {
				err = errors.Wrap(err, "Error while asserting ObjectID")
				return err
			}
		}
	}

	if m["flashID"] != nil {
		i.FlashID, err = uuuid.FromString(m["flashID"].(string))
		if err != nil {
			err = errors.Wrap(err, "Error while asserting FlashID")
			return err
		}
	}

	if m["itemID"] != nil {
		i.ItemID, err = uuuid.FromString(m["itemID"].(string))
		if err != nil {
			err = errors.Wrap(err, "Error while asserting ItemID")
			return err
		}
	}

	if m["deviceID"] != nil {
		i.DeviceID, err = uuuid.FromString(m["deviceID"].(string))
		if err != nil {
			err = errors.Wrap(err, "Error while asserting DeviceID")
			return err
		}
	}

	if m["lot"] != nil {
		i.Lot, assertOK = m["lot"].(string)
		if !assertOK {
			return errors.New("Error while asserting Lot")
		}
	}
	if m["name"] != nil {
		i.Name, assertOK = m["name"].(string)
		if !assertOK {
			return errors.New("Error while asserting Name")
		}
	}
	if m["origin"] != nil {
		i.Origin, assertOK = m["origin"].(string)
		if !assertOK {
			return errors.New("Error while asserting Origin")
		}
	}
	if m["price"] != nil {
		i.Price, err = util.AssertFloat64(m["price"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting Price")
			return err
		}
	}

	if m["salePrice"] != nil {
		i.SalePrice, err = util.AssertFloat64(m["salePrice"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting SalePrice")
			return err
		}
	}
	if m["sku"] != nil {
		i.SKU, assertOK = m["sku"].(string)
		if !assertOK {
			return errors.New("Error while asserting Sku")
		}
	}
	if m["soldWeight"] != nil {
		i.SoldWeight, err = util.AssertFloat64(m["soldWeight"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting SoldWeight")
			return err
		}
	}
	if m["timestamp"] != nil {
		i.Timestamp, err = util.AssertInt64(m["timestamp"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting Timestamp")
			return err
		}
	}
	if m["totalWeight"] != nil {
		i.TotalWeight, err = util.AssertFloat64(m["totalWeight"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting TotalWeight")
			return err
		}
	}
	if m["upc"] != nil {
		i.UPC, err = util.AssertInt64(m["upc"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting UPC")
			return err
		}
	}
	if m["ethylene"] != nil {
		i.Ethylene, err = util.AssertFloat64(m["ethylene"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting Ethylene")
			return err
		}
	}
	if m["status"] != nil {
		i.Status, assertOK = m["status"].(string)
		if !assertOK {
			return errors.New("Error while asserting Status")
		}
	}

	return nil
}
