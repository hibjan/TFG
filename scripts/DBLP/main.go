package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// --- JSON Output Structs ---
type Reference struct {
	Reason                string `json:"reason"`
	ReferenceID           int    `json:"reference_id"`
	ReferenceCollectionID int    `json:"reference_collection_id"`
}

type PMSObject struct {
	ID           int                 `json:"id"`
	CollectionID int                 `json:"collection_id"`
	Metadata     map[string][]string `json:"metadata"`
	References   []Reference         `json:"references"`
	Contents     map[string]any      `json:"contents"`
}

type MetadataFieldConfig struct {
	Type       string            `json:"type"` // "string", "numeric", "date", "coded"
	URI        string            `json:"uri"`
	Range      int               `json:"range"`      // bucket size for numeric
	RangeYear  int               `json:"range_year"` // bucket size for date years
	RangeMonth int               `json:"range_month"`
	RangeDay   int               `json:"range_day"`
	Code       map[string]string `json:"code"` // code->label for coded fields
}

type ContentRefConfig struct {
	URI string `json:"uri"`
}

type ResourceConfig struct {
	URI     string `json:"uri"`
	Type    string `json:"type"`     // "link", "doi"
	BaseURL string `json:"base_url"` // optional prefix
}

type ObjectConfig struct {
	CollectionID int                            `json:"collection_id"`
	Metadata     map[string]MetadataFieldConfig `json:"metadata"`
	Contents     map[string]ContentRefConfig    `json:"contents"`
	Resources    map[string]ResourceConfig      `json:"resources"`
	References   map[string]ContentRefConfig    `json:"references"`
}

type CollectionConfig struct {
	Name string   `json:"name"`
	ID   int      `json:"id"`
	URIs []string `json:"uris"`
}

type Config struct {
	Collections []CollectionConfig `json:"collections"`
	Objects     []ObjectConfig     `json:"objects"`
}

type PredMapping struct {
	IsMeta       bool
	MetaCfg      MetadataFieldConfig
	MetaName     string
	IsContent    bool
	ContentName  string
	IsResource   bool
	ResourceName string
	ResourceCfg  ResourceConfig
	IsRef        bool
	RefName      string
}

type EntityInfo struct {
	ID           int
	CollectionID int
}

// Number of buckets to split the file into.
// 64 is a great sweet spot. A 50GB file becomes 64 ~780MB files.
const numShards = 64

// Global toggle: set to true to only parse entities affiliated with UCM (.ucm.es)
var onlyUCM = true

var typePred = []byte("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <dblp.nt.gz> <format.json>")
		os.Exit(1)
	}
	rdfFilePath := os.Args[1]
	configFilePath := os.Args[2]

	// Load Configuration
	configFile, err := os.ReadFile(configFilePath)
	if err != nil {
		panic(fmt.Errorf("error reading config: %v", err))
	}

	var config Config
	if err := json.Unmarshal(configFile, &config); err != nil {
		panic(fmt.Errorf("error parsing config: %v", err))
	}

	idCounters := make(map[int]int)
	uriToColID := make(map[string]int)

	for _, coll := range config.Collections {
		idCounters[coll.ID] = 1
		for _, uri := range coll.URIs {
			uriToColID[uri] = coll.ID
		}
	}

	mappings := make(map[int]map[string]PredMapping)
	for _, obj := range config.Objects {
		cmap := make(map[string]PredMapping)
		for name, meta := range obj.Metadata {
			cmap[meta.URI] = PredMapping{
				IsMeta:   true,
				MetaCfg:  meta,
				MetaName: name,
			}
		}
		for name, cnt := range obj.Contents {
			pm := cmap[cnt.URI]
			pm.IsContent = true
			pm.ContentName = name
			cmap[cnt.URI] = pm
		}
		for name, res := range obj.Resources {
			pm := cmap[res.URI]
			pm.IsResource = true
			pm.ResourceName = name
			pm.ResourceCfg = res
			cmap[res.URI] = pm
		}
		for name, ref := range obj.References {
			pm := cmap[ref.URI]
			pm.IsRef = true
			pm.RefName = name
			cmap[ref.URI] = pm
		}
		mappings[obj.CollectionID] = cmap
	}

	// --- PHASE 1: DIRECT GZIP READING, DISCOVERY & SHARDING ---
	fmt.Println("Starting Phase 1: Reading .gz, building ID map, and sharding...")
	phase1Start := time.Now()
	uriToEntity := make(map[string]EntityInfo)

	// Open the compressed .gz file directly (saves extracting 50GB!)
	file, err := os.Open(rdfFilePath)
	if err != nil {
		panic(fmt.Errorf("error opening .gz file: %v", err))
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		panic(fmt.Errorf("error creating gzip reader: %v", err))
	}
	defer gzReader.Close()

	// Create our 64 temporary bucket files on disk
	shardFiles := make([]*os.File, numShards)
	shardWriters := make([]*bufio.Writer, numShards)
	for i := 0; i < numShards; i++ {
		sf, err := os.Create(fmt.Sprintf("shard_%d.nt", i))
		if err != nil {
			panic(err)
		}
		shardFiles[i] = sf
		shardWriters[i] = bufio.NewWriter(sf)
	}

	// Read schema.nt for inverse relations if it exists
	inverseMap := make(map[string]string)
	if schemaBytes, err := os.ReadFile("schema.nt"); err == nil {
		fmt.Println("Parsing schema.nt for inverse relations...")
		scanner := bufio.NewScanner(bytes.NewReader(schemaBytes))
		inversePred := []byte("http://www.w3.org/2002/07/owl#inverseOf")
		for scanner.Scan() {
			s, p, o, isURI := parseTripleBytes(scanner.Bytes())
			if isURI && bytes.Equal(p, inversePred) {
				inverseMap[string(s)] = string(o)
				inverseMap[string(o)] = string(s)
			}
		}
		fmt.Printf("Loaded %d inverse relations from schema.nt\n", len(inverseMap)/2)
	}

	scanner := bufio.NewScanner(gzReader)
	buf := make([]byte, 1024*1024) // 1MB buffer for long lines
	scanner.Buffer(buf, 1024*1024)

	lineCount := 0
	for scanner.Scan() {
		lineCount++
		if lineCount%5000000 == 0 {
			elapsed := time.Since(phase1Start)
			rate := float64(lineCount) / elapsed.Seconds()
			fmt.Printf("Phase 1: Processed %d million lines (%.1f/sec)\n", lineCount/1000000, rate)
		}

		line := scanner.Bytes()
		sub, pred, obj, isURI := parseTripleBytes(line)
		if len(sub) == 0 {
			continue
		}

		// 1. Discovery: Assign Integer IDs based on configuration
		if bytes.Equal(pred, typePred) {
			if collectionID, exists := uriToColID[string(obj)]; exists {
				uriToEntity[string(sub)] = EntityInfo{
					ID:           idCounters[collectionID],
					CollectionID: collectionID,
				}
				idCounters[collectionID]++
			}
		}

		// 2. Sharding: Hash the subject to determine which bucket this line belongs in
		shardIdx := getShardBytes(sub, numShards)
		shardWriters[shardIdx].Write(line)
		shardWriters[shardIdx].WriteByte('\n')

		// 3. Inverse Relations Handling
		if isURI {
			if invP, ok := inverseMap[string(pred)]; ok {
				targetIdx := getShardBytes(obj, numShards)
				w := shardWriters[targetIdx]
				w.WriteByte('<')
				w.Write(obj)
				w.WriteString("> <")
				w.WriteString(invP)
				w.WriteString("> <")
				w.Write(sub)
				w.WriteString("> .\n")
			}
		}
	}

	// Flush and close all bucket files
	for i := 0; i < numShards; i++ {
		shardWriters[i].Flush()
		shardFiles[i].Close()
	}

	fmt.Printf("Phase 1 complete! Total lines scanned: %d (took %v)\n", lineCount, time.Since(phase1Start).Round(time.Second))

	// --- PHASE 2: PROCESS SHARDS IN RAM AND DELETE ---
	fmt.Println("Starting Phase 2: Processing shards into JSON and cleaning up...")
	phase2Start := time.Now()

	tempFileName := "dblp_temp.jsonl"
	tempFile, err := os.Create(tempFileName)
	if err != nil {
		panic(err)
	}
	tempEncoder := json.NewEncoder(tempFile)
	tempEncoder.SetEscapeHTML(false)

	objectCount := 0

	// Process one bucket at a time
	for i := 0; i < numShards; i++ {
		if i > 0 && i%10 == 0 { // Print ETA every 10 shards
			elapsed := time.Since(phase2Start)
			rate := float64(i) / elapsed.Seconds()
			remaining := float64(numShards-i) / rate
			eta := time.Duration(remaining) * time.Second
			fmt.Printf("Phase 2: Processed %d/%d shards (%.1f/sec) - ETA: %v\n", i, numShards, rate, eta.Round(time.Second))
		}
		shardName := fmt.Sprintf("shard_%d.nt", i)
		fmt.Printf("Processing %s...\n", shardName)

		shardFile, err := os.Open(shardName)
		if err != nil {
			continue
		}

		// Because the bucket is small, we can hold all its objects in RAM!
		// We no longer need the file to be alphabetically sorted.
		objectsInRAM := make(map[string]*PMSObject)
		entityHasUCM := make(map[string]bool)

		shardScanner := bufio.NewScanner(shardFile)
		shardScanner.Buffer(make([]byte, 1024*1024), 1024*1024)

		for shardScanner.Scan() {
			sub, pred, obj, _ := parseTripleBytes(shardScanner.Bytes())
			if len(sub) == 0 {
				continue
			}

			// If this subject isn't one we mapped in Phase 1, ignore it
			entityInfo, tracked := uriToEntity[string(sub)]
			if !tracked {
				continue
			}

			subjectStr := string(sub)

			// Get or Create the object in RAM
			currentObj, exists := objectsInRAM[subjectStr]
			if !exists {
				currentObj = &PMSObject{
					ID:           entityInfo.ID,
					CollectionID: entityInfo.CollectionID,
					Metadata:     make(map[string][]string),
					References:   make([]Reference, 0),
					Contents:     make(map[string]any),
				}
				objectsInRAM[subjectStr] = currentObj
			}

			objMap, hasMap := mappings[entityInfo.CollectionID]
			if !hasMap {
				continue
			}

			fieldCfg, hasMapping := objMap[string(pred)]
			if !hasMapping {
				continue
			}

			objStr := string(obj)

			// Map data based on format.json
			if fieldCfg.IsMeta {
				processMetadataValue(currentObj, fieldCfg.MetaName, fieldCfg.MetaCfg, objStr)
			}
			if fieldCfg.IsContent {
				currentObj.Contents[fieldCfg.ContentName] = objStr
			}
			if fieldCfg.IsResource {
				url := objStr
				resType := fieldCfg.ResourceCfg.Type
				if resType == "doi" {
					if !strings.HasPrefix(objStr, "https://doi.org/") {
						url = "https://doi.org/" + objStr
					}
					resType = "link"
				}
				if fieldCfg.ResourceCfg.BaseURL != "" {
					url = fieldCfg.ResourceCfg.BaseURL + objStr
				}
				resEntry := map[string]string{
					"type":  resType,
					"label": fieldCfg.ResourceName,
					"url":   url,
				}
				existing, ok := currentObj.Contents["_resources"]
				if ok {
					if resList, ok2 := existing.([]any); ok2 {
						currentObj.Contents["_resources"] = append(resList, resEntry)
					}
				} else {
					currentObj.Contents["_resources"] = []any{resEntry}
				}
			}
			if fieldCfg.IsRef {
				if targetEntity, targetTracked := uriToEntity[objStr]; targetTracked {
					currentObj.References = append(currentObj.References, Reference{
						Reason:                fieldCfg.RefName,
						ReferenceID:           targetEntity.ID,
						ReferenceCollectionID: targetEntity.CollectionID,
					})
				}
			}

			// Check for '.ucm.es' in People objects mapping fields
			if onlyUCM && currentObj.CollectionID == 1 {
				if strings.Contains(strings.ToLower(objStr), ".ucm.es") {
					entityHasUCM[subjectStr] = true
				}
			}
		}
		shardFile.Close()

		// Write all objects from this shard to the temp file (no pruning yet)
		for sub, obj := range objectsInRAM {
			keep := true
			if onlyUCM && obj.CollectionID == 1 {
				keep = entityHasUCM[sub]
			}
			if keep {
				tempEncoder.Encode(obj)
				objectCount++
			}
		}

		// Delete the bucket file from the hard drive immediately to save space
		os.Remove(shardName)
	}
	tempFile.Close()

	fmt.Printf("Phase 2 complete! %d objects written to temp file (took %v)\n", objectCount, time.Since(phase2Start).Round(time.Second))

	// --- PHASE 3: CLEANUP (remove dangling references + prune empty objects) ---
	fmt.Println("Starting Phase 3: Cleaning up dangling references and empty objects...")
	phase3Start := time.Now()

	// Scan 1: Build set of all existing (collection_id, id) pairs
	existingEntities := make(map[[2]int]bool)
	tempRead, err := os.Open(tempFileName)
	if err != nil {
		panic(err)
	}
	scan1 := bufio.NewScanner(tempRead)
	scan1.Buffer(make([]byte, 1024*1024), 128*1024*1024) // Increase max size to 128MB
	for scan1.Scan() {
		var obj PMSObject
		if err := json.Unmarshal(scan1.Bytes(), &obj); err != nil {
			continue
		}
		existingEntities[[2]int{obj.CollectionID, obj.ID}] = true
	}
	if err := scan1.Err(); err != nil {
		fmt.Printf("Warning: Phase 3 Scan 1 ended with error: %v\n", err)
	}
	tempRead.Close()
	fmt.Printf("  Scan 1: Found %d entities\n", len(existingEntities))

	// Scan 2: Filter dangling references and prune empty objects
	tempRead2, err := os.Open(tempFileName)
	if err != nil {
		panic(err)
	}
	scan2 := bufio.NewScanner(tempRead2)
	scan2.Buffer(make([]byte, 1024*1024), 128*1024*1024) // Increase max size to 128MB

	outFile, err := os.Create("dblp_output.jsonl")
	if err != nil {
		panic(err)
	}
	outWriter := bufio.NewWriter(outFile)
	finalEncoder := json.NewEncoder(outWriter)
	finalEncoder.SetEscapeHTML(false)

	// Write collections header as first JSONL line (required by populate_db_jsonl.py)
	type CollectionEntry struct {
		Name string `json:"name"`
		ID   int    `json:"id"`
	}
	var collEntries []CollectionEntry
	seenCols := make(map[int]bool)
	for _, coll := range config.Collections {
		if !seenCols[coll.ID] {
			seenCols[coll.ID] = true
			collEntries = append(collEntries, CollectionEntry{Name: coll.Name, ID: coll.ID})
		}
	}
	finalEncoder.Encode(map[string]any{"collections": collEntries})

	written := 0
	pruned := 0
	danglingRefsRemoved := 0

	for scan2.Scan() {
		var obj PMSObject
		if err := json.Unmarshal(scan2.Bytes(), &obj); err != nil {
			continue
		}

		// Strip dangling references
		cleanRefs := make([]Reference, 0, len(obj.References))
		for _, ref := range obj.References {
			if existingEntities[[2]int{ref.ReferenceCollectionID, ref.ReferenceID}] {
				cleanRefs = append(cleanRefs, ref)
			} else {
				danglingRefsRemoved++
			}
		}
		obj.References = cleanRefs

		// Prune objects with no metadata AND no references AND no contents
		if len(obj.Metadata) == 0 && len(obj.References) == 0 && len(obj.Contents) == 0 {
			pruned++
			continue
		}

		// Drop works (CollectionID == 2) that no longer have a connection to any Person (CollectionID == 1)
		// Assuming we only want to keep works related to UCM. Since only UCM People remain,
		// any work that no longer references a Person should be dropped.
		if onlyUCM && obj.CollectionID == 2 {
			hasValidPersonRef := false
			for _, ref := range obj.References {
				if ref.ReferenceCollectionID == 1 {
					hasValidPersonRef = true
					break
				}
			}
			if !hasValidPersonRef {
				pruned++
				continue
			}
		}

		finalEncoder.Encode(obj)
		written++
	}
	if err := scan2.Err(); err != nil {
		fmt.Printf("Warning: Phase 3 Scan 2 ended with error: %v\n", err)
	}
	outWriter.Flush()
	outFile.Close()
	tempRead2.Close()

	// Delete temp file
	os.Remove(tempFileName)

	fmt.Printf("  Scan 2: Wrote %d objects, pruned %d empty, removed %d dangling references\n", written, pruned, danglingRefsRemoved)
	fmt.Printf("DBLP Parsing fully complete! Check dblp_output.jsonl (took %v)\n", time.Since(phase3Start).Round(time.Second))
}

// --- Helper Functions ---

// processMetadataValue processes a raw RDF value according to the field's type config
// and populates both metadata and contents on the object (matching process.py behavior).
func processMetadataValue(obj *PMSObject, name string, cfg MetadataFieldConfig, rawValue string) {
	switch cfg.Type {
	case "numeric":
		rangeSize := cfg.Range
		if rangeSize <= 0 {
			rangeSize = 1000
		}
		r := bucketRange(rawValue, rangeSize)
		if r != "" {
			obj.Metadata[name] = append(obj.Metadata[name], r)
		}
		obj.Contents[name+" (raw)"] = rawValue

	case "date":
		rangeYear := cfg.RangeYear
		if rangeYear <= 0 {
			rangeYear = 10
		}
		rangeMonth := cfg.RangeMonth
		if rangeMonth <= 0 {
			rangeMonth = 1
		}
		rangeDay := cfg.RangeDay
		if rangeDay <= 0 {
			rangeDay = 1
		}

		parts := strings.Split(rawValue, "-")
		if len(parts) == 0 || parts[0] == "" {
			return
		}
		obj.Contents[name+" (raw)"] = rawValue

		// Year
		y, err := strconv.Atoi(parts[0])
		if err != nil {
			return
		}
		yStart := (y / rangeYear) * rangeYear
		yEnd := yStart + rangeYear - 1
		obj.Metadata[name+" (Year)"] = append(obj.Metadata[name+" (Year)"], fmt.Sprintf("%d-%d", yStart, yEnd))

		// Month
		if len(parts) >= 2 {
			m, err := strconv.Atoi(parts[1])
			if err == nil {
				mStart := ((m-1)/rangeMonth)*rangeMonth + 1
				mEnd := mStart + rangeMonth - 1
				if mEnd > 12 {
					mEnd = 12
				}
				if mStart == mEnd {
					obj.Metadata[name+" (Month)"] = append(obj.Metadata[name+" (Month)"], strconv.Itoa(mStart))
				} else {
					obj.Metadata[name+" (Month)"] = append(obj.Metadata[name+" (Month)"], fmt.Sprintf("%d-%d", mStart, mEnd))
				}
			}
		}

		// Day
		if len(parts) >= 3 {
			d, err := strconv.Atoi(parts[2])
			if err == nil {
				dStart := ((d-1)/rangeDay)*rangeDay + 1
				dEnd := dStart + rangeDay - 1
				if dEnd > 31 {
					dEnd = 31
				}
				if dStart == dEnd {
					obj.Metadata[name+" (Day)"] = append(obj.Metadata[name+" (Day)"], strconv.Itoa(dStart))
				} else {
					obj.Metadata[name+" (Day)"] = append(obj.Metadata[name+" (Day)"], fmt.Sprintf("%d-%d", dStart, dEnd))
				}
			}
		}

	case "coded":
		if cfg.Code != nil {
			if label, ok := cfg.Code[rawValue]; ok {
				rawValue = label
			}
		}
		obj.Metadata[name] = append(obj.Metadata[name], rawValue)

	default: // "string" or unrecognized
		obj.Metadata[name] = append(obj.Metadata[name], rawValue)
	}
}

// formatNumber formats a number with human-readable suffixes: K, M, B
func formatNumber(n int) string {
	neg := n < 0
	if neg {
		n = -n
	}

	var s string
	switch {
	case n >= 1_000_000_000:
		s = formatWithSuffix(n, 1_000_000_000, "B")
	case n >= 1_000_000:
		s = formatWithSuffix(n, 1_000_000, "M")
	case n >= 1_000:
		s = formatWithSuffix(n, 1_000, "K")
	default:
		s = strconv.Itoa(n)
	}

	if neg {
		return "-" + s
	}
	return s
}

func formatWithSuffix(n, divisor int, suffix string) string {
	whole := n / divisor
	frac := n % divisor
	if frac == 0 {
		return fmt.Sprintf("%d%s", whole, suffix)
	}
	// Build decimal, strip trailing zeros, max 3 digits
	fracStr := fmt.Sprintf("%0*d", len(strconv.Itoa(divisor))-1, frac)
	fracStr = strings.TrimRight(fracStr, "0")
	if len(fracStr) > 3 {
		fracStr = fracStr[:3]
	}
	return fmt.Sprintf("%d.%s%s", whole, fracStr, suffix)
}

// bucketRange returns a human-readable range string for a numeric value
func bucketRange(value string, bucketSize int) string {
	num, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return ""
	}

	if num < 0 {
		bucketStart := -bucketSize * (int(math.Abs(num)/float64(bucketSize)) + 1)
		return formatNumber(bucketStart) + "-" + formatNumber(bucketStart+bucketSize-1)
	}

	bucketStart := int(num/float64(bucketSize)) * bucketSize
	return formatNumber(bucketStart) + "-" + formatNumber(bucketStart+bucketSize-1)
}

const offset32 = 2166136261
const prime32 = 16777619

// Creates a consistent numeric hash for a byte slice to assign it to a shard
func getShardBytes(subject []byte, maxShards int) int {
	hash := uint32(offset32)
	for _, c := range subject {
		hash ^= uint32(c)
		hash *= prime32
	}
	return int(hash) % maxShards
}

// Robust byte-level parsing logic to handle complex strings and datatypes in DBLP, avoids allocation
func parseTripleBytes(line []byte) (sub []byte, pred []byte, obj []byte, isURI bool) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] == '#' {
		return nil, nil, nil, false
	}

	part1Idx := bytes.IndexByte(line, ' ')
	if part1Idx == -1 {
		return nil, nil, nil, false
	}
	sub = bytes.Trim(line[:part1Idx], "<>")

	line = bytes.TrimSpace(line[part1Idx:])
	part2Idx := bytes.IndexByte(line, ' ')
	if part2Idx == -1 {
		return nil, nil, nil, false
	}
	pred = bytes.Trim(line[:part2Idx], "<>")

	objBytes := bytes.TrimSpace(line[part2Idx:])
	objBytes = bytes.TrimSuffix(objBytes, []byte("."))
	objBytes = bytes.TrimSpace(objBytes)

	if len(objBytes) > 0 && objBytes[0] == '<' && objBytes[len(objBytes)-1] == '>' {
		isURI = true
		obj = objBytes[1 : len(objBytes)-1]
	} else if len(objBytes) > 0 && objBytes[0] == '"' {
		isURI = false
		lastQuote := bytes.LastIndexByte(objBytes, '"')
		if lastQuote > 0 {
			// Extract the fully quoted string (e.g. "Jos\u00e9")
			quotedStr := string(objBytes[:lastQuote+1])

			// Use strconv.Unquote to parse \uXXXX escapes into native UTF-8
			if unquoted, err := strconv.Unquote(quotedStr); err == nil {
				obj = []byte(unquoted)
			} else {
				// Fallback if it's a malformed string
				obj = objBytes[1:lastQuote]
			}
		} else {
			obj = objBytes
		}
	} else {
		isURI = false
		obj = objBytes
	}

	return sub, pred, obj, isURI
}
