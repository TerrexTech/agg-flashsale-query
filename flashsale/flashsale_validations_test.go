package flashsale

import (
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestFlashsale only tests basic pre-processing error-checks for Aggregate functions.
func TestFlashsale(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FlashsaleAggregate Suite")
}

var _ = Describe("FlashsaleAggregate", func() {
	Describe("query", func() {
		It("should return error if filter is empty", func() {
			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				Action:        "query",
				CorrelationID: cid,
				AggregateID:   7,
				Data:          []byte("{}"),
				Timestamp:     time.Now(),
				UserUUID:      uid,
				TimeUUID:      timeUUID,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Query(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))
		})
	})
})
