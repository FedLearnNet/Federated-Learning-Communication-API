package util

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"

	"github.com/imdario/mergo"
	"gopkg.in/yaml.v2"
)

// LoadConfig updates configuration in order of decreasing priority:
// 1. CLI flags (non-zero values only)
// 2. OS environment variables
// 3. config.yml file (if it exists)
// 4. defaults from the provided defaultConfig
//
// The Config struct should use struct tags:
//   - yaml:"fieldname"     (for YAML parsing)
//   - long:"flagname"      (for CLI flags)
//   - env:"ENVVAR_NAME"    (for environment variables)
//
// Parameters:
//   - configPath: path to config.yml file
//   - defaultConfig: Config struct with defaults already set
//   - cliConfig: Config struct with CLI-parsed values
//
// Returns the merged configuration.
func LoadConfig(configPath string, defaultConfig interface{}, cliConfig interface{}) (interface{}, error) {
	// We just go through decreasing priority overwriting each step
	// Defaults are given, so loaded already

	// Load YAML config file and overlay on top of defaults (optional)
	abs, _ := filepath.Abs(configPath)
	yamlData, err := os.ReadFile(abs)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("config file read error: %w", err)
	}
	if os.IsNotExist(err) {
		fmt.Printf("Config file not found at %s, proceeding with defaults and CLI options\n", abs)
	} else {
		// Overwriting defaults with YAML config
		if err := yaml.Unmarshal(yamlData, defaultConfig); err != nil {
			return nil, fmt.Errorf("config file structural error: %w", err)
		}
	}

	// Overlay environment variables
	applyEnvVars(defaultConfig)

	// Overlay CLI options (only non-zero values)
	if cliConfig != nil {
		if err := mergo.Merge(defaultConfig, cliConfig, mergo.WithOverride); err != nil {
			return nil, fmt.Errorf("config merge error (CLI): %w", err)
		}
	}

	return defaultConfig, nil
}

// PrintConfig prints all fields of any config struct using yaml tags as field names.
func PrintConfig(cfg interface{}) {
	fmt.Println("\nLoaded Configuration:")
	v := reflect.ValueOf(cfg)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	printStructFields(v, "")
}

func formatValue(v reflect.Value) string {
	if !v.IsValid() {
		return "<nil>"
	}

	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return "<nil>"
		}
		return formatValue(v.Elem())
	case reflect.String:
		return v.String()
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Slice, reflect.Array, reflect.Map:
		if v.IsNil() {
			return "<nil>"
		}
		return fmt.Sprintf("%v", v.Interface())
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}

func printStructFields(v reflect.Value, indent string) {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		name := fieldType.Tag.Get("yaml")
		if name == "" {
			name = fieldType.Name
		}
		if field.Kind() == reflect.Struct {
			fmt.Printf("%s%s:\n", indent, name)
			printStructFields(field, indent+"  ")
			continue
		}
		if field.Kind() == reflect.Ptr && !field.IsNil() && field.Elem().Kind() == reflect.Struct {
			fmt.Printf("%s%s:\n", indent, name)
			printStructFields(field.Elem(), indent+"  ")
			continue
		}
		fmt.Printf("%s%s: %s\n", indent, name, formatValue(field))
	}
}

// applyEnvVars applies environment variable overrides based on struct tags
// The struct must use `env:"ENV_VAR_NAME"` tags
func applyEnvVars(cfg interface{}) {
	if cfg == nil {
		return
	}

	v := reflect.ValueOf(cfg)
	// If it's a pointer, get the element
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return
	}

	// Recursively apply env vars to all struct fields
	applyEnvVarsToStruct(v)
}

// applyEnvVarsToStruct recursively applies environment variables to struct fields
func applyEnvVarsToStruct(v reflect.Value) {
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		// If it's a nested struct, recurse
		if field.Kind() == reflect.Struct {
			applyEnvVarsToStruct(field)
			continue
		}

		// Get the env tag
		envTag := fieldType.Tag.Get("env")
		if envTag == "" || envTag == "-" {
			continue
		}

		envValue := os.Getenv(envTag)
		if envValue == "" {
			continue
		}

		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			switch field.Elem().Kind() {
			case reflect.String:
				field.Elem().SetString(envValue)
			case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
				if intVal, err := strconv.ParseInt(envValue, 10, 64); err == nil {
					field.Elem().SetInt(intVal)
				}
			case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
				if uintVal, err := strconv.ParseUint(envValue, 10, 64); err == nil {
					field.Elem().SetUint(uintVal)
				}
			case reflect.Bool:
				if boolVal, err := strconv.ParseBool(envValue); err == nil {
					field.Elem().SetBool(boolVal)
				}
			case reflect.Float32, reflect.Float64:
				if floatVal, err := strconv.ParseFloat(envValue, 64); err == nil {
					field.Elem().SetFloat(floatVal)
				}
			}
			continue
		}

		// Set the field based on its type
		switch field.Kind() {
		case reflect.String:
			field.SetString(envValue)
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			if intVal, err := strconv.ParseInt(envValue, 10, 64); err == nil {
				field.SetInt(intVal)
			}
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			if uintVal, err := strconv.ParseUint(envValue, 10, 64); err == nil {
				field.SetUint(uintVal)
			}
		case reflect.Bool:
			if boolVal, err := strconv.ParseBool(envValue); err == nil {
				field.SetBool(boolVal)
			}
		case reflect.Float32, reflect.Float64:
			if floatVal, err := strconv.ParseFloat(envValue, 64); err == nil {
				field.SetFloat(floatVal)
			}
		}
	}
}
