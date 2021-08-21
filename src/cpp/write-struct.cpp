#include <iostream>
#include <sstream>
#include <string>
#include <memory>
#include <stdint.h>
#include <map>
#include <cmath>
#include <tuple>
#include <variant>
#include <type_traits>


#include "json.hpp"

// arrow/parquet
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "arrow/util/checked_cast.h"
#include <arrow/type.h> // struct_
#include <arrow/ipc/json_simple.h>

// just messing around with schema with arbitrary depths and getting
// used to the API

#define check_result(expression) \
    if (!expression.ok()) \
       throw std::logic_error(#expression);

std::shared_ptr<arrow::Table> generate_table() {
    using nlohmann::json;

    // lets make the following struct
    // {
    //    "node" : {
    //                "myIntVal" : int,
    //                "myFloatVal" : float
    //              }
    //


    auto pool = arrow::default_memory_pool();
    auto int_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto float_builder = std::make_shared<arrow::FloatBuilder>(pool);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders{int_builder, float_builder};
    std::vector fields{arrow::field("myIntVal", arrow::int32()), arrow::field("myFloatVal", arrow::float32())};
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(fields);

    auto struct_builder = std::make_shared<arrow::StructBuilder>(struct_type, pool, std::move(field_builders));

    // add some data
    check_result(struct_builder->Append());
    check_result(int_builder->Append(42));
    check_result(float_builder->Append(99.2));
    std::shared_ptr<arrow::Array> array;
    check_result(struct_builder->Finish(&array));


    std::shared_ptr<arrow::Schema> schema;
    schema = arrow::schema({arrow::field("myStruct", struct_type)});

	return arrow::Table::Make(schema, {array});

}

std::shared_ptr<arrow::Table> generate_table2() {
    using nlohmann::json;

    // lets make the following struct
    // {
    //    "myIntVal0" : int,
    //    "myFloatVal0" : float,
    //    "myStruct1" : {
    //                "myIntVal1" : int,
    //                "myFloatVal1" : float,
    //                "myStruct2" : {
    //                  "myIntVal2" : int,
    //                  "myFloatVal2" : float,
    //                  "myList2" : list[int]
    //                }
    //              }
    //


    auto pool = arrow::default_memory_pool();


    // create the inner struct
    auto builder_int2 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_float2 = std::make_shared<arrow::FloatBuilder>(pool);
    auto builder_listInt2 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_list2 = std::make_shared<arrow::ListBuilder>(pool, builder_listInt2);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders2{builder_int2, builder_float2, builder_list2};
    std::vector fields2{arrow::field("myIntVal2", arrow::int32()),
        arrow::field("myFloatVal2", arrow::float32()),
        arrow::field("myList2", arrow::list(arrow::int32()))
    };
    std::shared_ptr<arrow::DataType> type_struct2 = arrow::struct_(fields2);
    auto builder_struct2 = std::make_shared<arrow::StructBuilder>(type_struct2, pool, std::move(builders2));

    // create the outer struct
    auto builder_int1 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_float1 = std::make_shared<arrow::FloatBuilder>(pool);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders1{builder_int1, builder_float1, builder_struct2};
    std::vector fields1{arrow::field("myIntVal", arrow::int32()),
                        arrow::field("myFloatVal", arrow::float32()),
                        arrow::field("myStruct2", type_struct2)
    };
    std::shared_ptr<arrow::DataType> type_struct1 = arrow::struct_(fields1);

    auto builder_struct1 = std::make_shared<arrow::StructBuilder>(type_struct1, pool, std::move(builders1));

    // create the top level flat fields
    auto builder_int0 = std::make_shared<arrow::Int32Builder>(pool);
    auto builder_float0 = std::make_shared<arrow::FloatBuilder>(pool);
    auto field_int0 = arrow::field("myInt0", arrow::int32());
    auto field_float0 = arrow::field("myFloat0", arrow::float32());


    // add data
    check_result(builder_int0->Append(0));
    check_result(builder_float0->Append(0.5));

    check_result(builder_struct1->Append());
    check_result(builder_int1->Append(1));
    check_result(builder_float1->Append(1.5));

    check_result(builder_struct2->Append());
    check_result(builder_int2->Append(2));
    check_result(builder_float2->Append(2.5));
    check_result(builder_list2->Append());
    check_result(builder_listInt2->AppendValues({3,4,5}));


    std::shared_ptr<arrow::Array> struct_array;
    check_result(builder_struct1->Finish(&struct_array));

    std::shared_ptr<arrow::Array> int0_array;
    std::shared_ptr<arrow::Array> float0_array;
    check_result(builder_int0->Finish(&int0_array));
    check_result(builder_float0->Finish(&float0_array));

    std::shared_ptr<arrow::Schema> schema;
    schema = arrow::schema({arrow::field("myInt0", arrow::int32()),
            arrow::field("myFloat0", arrow::float32()),
            arrow::field("myStruct1", type_struct1)
    });
    std::cout << schema->ToString() << std::endl;

	return arrow::Table::Make(schema, {int0_array, float0_array, struct_array});

}

// terminal node

template<typename>
struct is_std_vector : std::false_type {};

template<typename T, typename A>
struct is_std_vector<std::vector<T,A>> : std::true_type {};

template<typename>
struct is_int_vector : std::false_type {};

template<typename T>
struct getType {
	typedef T type;
};

template<typename T, typename A>
struct getType<std::vector<T, A>> {
	typedef T type;
};



//template<typename>
//struct type_from_vector {
//	typedef void value_type;
//};
//
//template<typename T, typename A>
//struct type_from_vector<std::vector<T,A>> {
//	typedef typename(T) value_type;
//};


//class StructDescriptor {
//	public :
//		StructDescriptor(std::string name) : _name(name) {}
//
//	private :
//		std::string _name;
//		std::vector<std::string> _field_names;
//};
//
//#define MyStruct(name

//    union data_buffer_t
//    {
//      int u_int;
//      unsigned int u_uint;
//      float u_float;
//      double u_double;
//	  std::vector<int> u_intvec;
//	  std::vector<float> u_floatvec;
//      bool u_bool;
//    };

typedef std::variant<int, float, std::vector<int>> data_variant;

struct DataVariantVisitor
{
	void operator()(int i) const {
		std::cout << "DataVariantVisitor -> int: " << i << std::endl;
	}
	void operator()(float f) const {
		std::cout << "DataVariantVisitor -> float: " << f << std::endl;
	}
	void operator()(std::vector<int> v) {
		std::cout << "DataVariantVisitor -> vector<int>: ";
		std::stringstream sx;
		for(const auto& x : v) {
			sx << " " << x;
		}
		std::cout << sx.str() << std::endl;
	}
};


template <typename I>
class Node {

    public :
        Node(std::string name) : _name(name), _is_struct_type(false), _is_list_type(false),  _builder(nullptr) {
			if constexpr(std::is_convertible<I*, arrow::NestedType*>::value) {
				_is_struct_type = true;
			}
			if constexpr(is_std_vector<I>::value) {
				_is_list_type = true;
			}
		}
		arrow::ArrayBuilder* builder() { return _builder; }
		std::string name() { return _name; }
		bool isList() { return _is_list_type; }
		bool isStruct() { return _is_struct_type; }
        void createBuilder() {
            auto pool = arrow::default_memory_pool();

			if constexpr(std::is_integral<I>::value) {
				std::cout << "createBuilder[" << _name << "] int" << std::endl;
				std::unique_ptr<arrow::ArrayBuilder> tmp;
				check_result(arrow::MakeBuilder(pool, arrow::int32(), &tmp));
				_builder = tmp.release();
			} else
			if constexpr(std::is_floating_point<I>::value) {
				std::cout << "createBuilder[" << _name << "] float" << std::endl;
				std::unique_ptr<arrow::ArrayBuilder> tmp;
				check_result(arrow::MakeBuilder(pool, arrow::float32(), &tmp));
				_builder = tmp.release();
			} else
			if constexpr(is_std_vector<I>::value) {
				std::cout << "createBuilder[" << _name << "]" << std::endl;
				typedef typename getType<I>::type InnerType;
				if constexpr(std::is_integral<InnerType>::value) {
					std::cout << "           -> vector int" << std::endl;
					std::unique_ptr<arrow::ArrayBuilder> tmp;
					check_result(arrow::MakeBuilder(pool, arrow::list(arrow::int32()), &tmp));
					_builder = tmp.release();
					std::cout << "      vectr type created builder with type " << _builder->type()->name() << std::endl;
				} else
				if constexpr(std::is_floating_point<InnerType>::value) {
					std::cout << "           -> vector float" << std::endl;
					std::unique_ptr<arrow::ArrayBuilder> tmp;
					check_result(arrow::MakeBuilder(pool, arrow::list(arrow::float32()), &tmp));
					_builder = tmp.release();
				} else
				static_assert(std::is_integral<I>::value ||
									std::is_floating_point<I>::value ||
									std::is_array<I>::value || is_std_vector<I>::value, "createBuilder: Must be a good type!");
			} else {
				return;
			}

        }

		void createBuilder(std::shared_ptr<arrow::DataType> type) {
			auto pool = arrow::default_memory_pool();

			std::cout << "createBuilder[" << _name << "] type: " << type->name() << std::endl;
			
			std::unique_ptr<arrow::ArrayBuilder> tmp;
			check_result(arrow::MakeBuilder(pool, type, &tmp));
			_builder = tmp.release();
		}
        virtual ~Node() = default;

		//virtual void fill(std::map<std::string, arrow::ArrayBuilder*> fill_map) {
		//	std::stringstream key;
		//	key << _name << "/";
		//	if(_is_nested) {
		//		_builder->Append();
		//		for(size_t i = 0; i < _child_builders.size(); i++) {
		//			_child_builders->Append(1);
		//		}
		//	}
		//}

        //virtual void fill(I val) {

		//	if constexpr(std::is_integral<I>::value || std::is_floating_point<I>::value) {
		//		check_result(_builder->Append(val));
		//	} else
		//	if constexpr(std::is_array<I>::value || is_std_vector<I>::value) {
		//		check_result(_builder->Append());
		//		check_result(_builder->AppendValues(val));
		//	} else 
		//	}
        //};
        void finish() {
            PARQUET_THROW_NOT_OK(_builder->Finish(_array));
        }
        std::shared_ptr<arrow::Array> getArray() { return _array; }

		void setBuilderMap(std::map<std::string, arrow::ArrayBuilder*>& map) {
			_child_builder_map = map;
		}

    private :
        std::string _name;
        //std::unique_ptr<arrow::ArrayBuilder> _builder;
		arrow::ArrayBuilder* _builder;
		std::vector<std::shared_ptr<arrow::ArrayBuilder>> _child_builders;
		std::shared_ptr<Node> _parent;
        std::shared_ptr<arrow::Array> _array;
		bool _is_struct_type;
		bool _is_list_type;
		std::map<std::string, arrow::ArrayBuilder*> _child_builder_map;
};


void
makeVariableMap(arrow::ArrayBuilder* builder, std::string parentname, std::string prefix,
std::map<std::string, arrow::ArrayBuilder*>& out_map) {

	auto type = builder->type();
	if(builder->num_children() > 0) {
		for(size_t ichild = 0; ichild < builder->num_children(); ichild++) {
			auto field = type->field(ichild);
			auto child_builder = builder->child_builder(ichild).get();
			auto child_type = child_builder->type();
			auto n_child_children = child_builder->num_children();
			bool child_is_nested = (child_builder->num_children() > 0);
			bool child_is_list = (child_type->id() == arrow::Type::LIST);
			if(child_is_nested) {
				std::string this_name = parentname + "/" + field->name() + "/";
				out_map[this_name] = child_builder;

				std::string child_name = parentname + "/" + field->name();
				makeVariableMap(child_builder, child_name, field->name(), out_map);
			} else if (child_is_list) {
				arrow::ListBuilder* list_builder = static_cast<arrow::ListBuilder*>(child_builder);
				auto item_builder = list_builder->value_builder();
				//std::cout << "BUTTS list_builder = " << list_builder << std::endl;
				//std::cout << "BUTTS item_builder = " << item_builder << std::endl;
				std::string outname = parentname + "/" + field->name();
				std::string list_name = outname + "/list";
				std::string val_name = outname + "/item";
				out_map[list_name] = child_builder;
				out_map[val_name] = item_builder; //dynamic_cast<arrow::ArrayBuilder*>(item_builder);
			} else {

				std::string outname = parentname + "/" + field->name();
				out_map[outname] = child_builder;
			}
			std::cout << "  [" << parentname << "][" << ichild << "] = " << field->name() << "/" << field->type()->name() << " nested? " << std::boolalpha << (n_child_children > 0) << std::endl;
		} // ichild
	} else if(type->id() == arrow::Type::LIST) {
		auto list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
		std::string outname = parentname;
		if(prefix != "") {
			outname = prefix + "/" + outname;
		}
		std::string list_name = outname + "/list";
		out_map[list_name] = list_builder;
		std::string val_name = outname + "/item";
		out_map[val_name] = list_builder->value_builder();
	} else {
		std::string outname = parentname;
		if(prefix != "") {
			outname = prefix + "/" + outname;
		}
		out_map[outname] = builder;
	}
}

template<typename T>
std::map<std::string, arrow::ArrayBuilder*>
makeVariableMap(std::shared_ptr<Node<T>> inode) {
	auto builder = inode->builder();
	std::map<std::string, arrow::ArrayBuilder*> out_map;
	makeVariableMap(builder, inode->name(), "", out_map);
	return out_map;
}

template<typename T>
void fill(T val, arrow::ArrayBuilder* builder) {
	if constexpr(is_std_vector<T>::value) {
		auto list = dynamic_cast<arrow::ListBuilder*>(builder);
		PARQUET_THROW_NOT_OK(list->Append());
		typedef typename getType<T>::type InnerType;
		if constexpr(std::is_integral<InnerType>::value) {
			std::cout << "FILLING VECTOR OF INT" << std::endl;
			PARQUET_THROW_NOT_OK(dynamic_cast<arrow::Int32Builder*>(list->value_builder())->AppendValues(val));
		} else
		if constexpr(std::is_floating_point<InnerType>::value) {
			std::cout << "FILLING VECTOR OF FLOAT" << std::endl;
			PARQUET_THROW_NOT_OK(dynamic_cast<arrow::FloatBuilder*>(list->value_builder())->AppendValues(val));
		}
	}
}

bool endsWith (std::string const &fullString, std::string const &ending) {
    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}


void fillStruct(std::map<std::string, arrow::ArrayBuilder*> builders, std::string struct_name,
	const std::map<std::string, data_variant>& val_map) {

	if(!endsWith(struct_name, "/")) struct_name += "/";
	auto struct_builder = builders.at(struct_name);
	std::vector<std::string> struct_fields;
	std::stringstream sx;
	for(auto & [name, val] : val_map) {
		sx.str();
		sx << struct_name << name;


		std::cout << "STRUCT " << struct_name << " name = " << sx.str() << ", val = ";

		if (std::holds_alternative<int>(val))  {
			std::cout << " int = " << std::get<int>(val) << std::endl;
		} else if(std::holds_alternative<float>(val)) {
			std::cout << " float = " << std::get<float>(val) << std::endl;
		} else if(std::holds_alternative<std::vector<int>>(val)) {
			auto intvec = std::get<std::vector<int>>(val);
			std::cout << " intvec = ";
			for(auto v : intvec) std::cout << " " << v;
			std::cout << std::endl;
		}
	}
}
	
//    union data_buffer_t
//    {
//      int u_int;
//      unsigned int u_uint;
//      float u_float;
//      double u_double;
//	  std::vector<int> u_intvec;
//	  std::vector<float> u_floatvec;
//      bool u_bool;
//    };

std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>>
//std::tuple<std::shared_ptr<arrow::ArrayBuilder>, std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>>
create_struct_builder() {
//std::shared_ptr<arrow::ArrayBuilder> create_struct_builder() {

    // create the inner struct
    //auto pool = arrow::default_memory_pool();
    //auto builder_int = std::make_shared<arrow::Int32Builder>(pool);
    //auto builder_float = std::make_shared<arrow::FloatBuilder>(pool);
    //auto builder_listInt = std::make_shared<arrow::Int32Builder>(pool);
    //auto builder_list = std::make_shared<arrow::ListBuilder>(pool, builder_listInt);
    //std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders{builder_int, builder_float, builder_list};
    std::vector fields{arrow::field("foo", arrow::int32()),
        arrow::field("bar", arrow::float32()),
        arrow::field("baz", arrow::list(arrow::int32())),
		arrow::field("inner_struct", arrow::struct_(
										{
										 arrow::field("inner-val", arrow::int32()),
										 arrow::field("inner_struct2", 
															arrow::struct_(
																{arrow::field("inner-val2", arrow::float64())}
															)
													  )
										}
									)
					)
    };
    std::shared_ptr<arrow::DataType> type_struct = arrow::struct_(fields);
    //auto builder_struct = std::make_shared<arrow::StructBuilder>(type_struct, pool, std::move(builders));

    using namespace arrow;
	std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> builder_map;

//	//auto int_node = std::make_shared<Node<uint32_t, arrow::Int32Builder>>("MyIntNode");
	auto int_node = std::make_shared<Node<int32_t>>("MyIntNode");
	int_node->createBuilder();
//	auto float_node = std::make_shared<Node<float>>("MyFloatNode");
//	float_node->createBuilder();
	auto intlist_node = std::make_shared<Node<std::vector<int32_t>>>("MyListIntNode");
	intlist_node->createBuilder();
	auto floatlist_node = std::make_shared<Node<std::vector<float>>>("MyListFloatNode");
	floatlist_node->createBuilder();

	auto int_node_map = makeVariableMap(int_node);
	builder_map["MyIntNode"] = int_node_map;

	auto float_list_node_map = makeVariableMap(floatlist_node);
	builder_map["MyFloatNode"] = float_list_node_map;

	std::cout << "------- INT NODE MAP -------" << std::endl;
	for(const auto& [key, val] : int_node_map) {
		std::cout << " key = " << key << ", val type = " << val->type()->name() << ", val num children = " << val->num_children() << std::endl;
	}
	//dynamic_cast<arrow::Int32Builder*>(int_node_map["MyIntNode"])->Append(4);


	auto struct_node = std::make_shared<Node<arrow::StructType>>("MyStructNode");
	struct_node->createBuilder(type_struct);
	auto struct_node_map = makeVariableMap(struct_node);
	std::cout << "------- STRUCT NODE MAP -------" << std::endl;
	for(const auto& [key, val] : struct_node_map) {
		auto builder = dynamic_cast<arrow::ArrayBuilder*>(val);
		std::cout << " key = " << key << ", val: " << val << std::endl; //",  val type: " << val->type()->name() << std::endl; //<< ", val num children = " << val->num_children() << std::endl;
		std::cout << "							-> type: " << val->type()->name() << std::endl;
	}

	//dynamic_cast<arrow::FloatBuilder*>(struct_node_map["MyStructNode/bar"])->Append(43.2);
	std::vector<int> vals{1,2,3};
	//fill<std::vector<int>>(vals, struct_node_map.at("MyStructNode/baz/list"));
	std::cout << "------- FLOATLIST NODE MAP -------" << std::endl;
	for(const auto& [key, val] : float_list_node_map) {
		auto builder = dynamic_cast<arrow::ArrayBuilder*>(val);
		std::cout << " key = " << key << ", val: " << val << std::endl; //",  val type: " << val->type()->name() << std::endl; //<< ", val num children = " << val->num_children() << std::endl;
		std::cout << "							-> type: " << val->type()->name() << std::endl;
	}

	std::vector<float> float_vals{1.2, 3.4, 7.9};
	fill<std::vector<float>>(float_vals, float_list_node_map.at("MyListFloatNode/list"));

//void fillStruct(std::map<std::string, arrow::ArrayBuilder*> builders, std::string struct_name,
//	const std::map<std::string, data_buffer_t>& val_map) {

	std::map<std::string, data_variant> struct_vals;
	data_variant val;
	val = 32;
	struct_vals["inner-val"] = val;
	std::visit(DataVariantVisitor{}, struct_vals["inner-val"]);
	fillStruct(struct_node_map, "MyStructNode/inner_struct/", struct_vals);

	float float_val = 32.07;
	val = float_val;
	struct_vals["inner-val"] = val;
	std::visit(DataVariantVisitor{}, struct_vals["inner-val"]);
	fillStruct(struct_node_map, "MyStructNode/inner_struct/", struct_vals);

	std::vector<int> intvec_val{1,2,3,4};
	struct_vals["inner-val"] = intvec_val;
	std::visit(DataVariantVisitor{}, struct_vals["inner-val"]);
	fillStruct(struct_node_map, "MyStructNode/inner_struct/", struct_vals);
	



	//dynamic_cast<arrow::ListBuilder*>(struct_node_map["MyStructNode/baz/list"])->Append();
//	dynamic_cast<arrow::ListBuilder*>(struct_node_map["MyStructNode/baz/item"])->Append(14);

	

	//auto list_builder = dynamic_cast<arrow::ListBuilder*>(struct_node_map["MyStructNode/baz/list"]);
	//auto value_builder = dynamic_cast<arrow::Int32Builder*>(struct_node_map["MyStructNode/baz/item"]);
	//list_builder->Append();
	//value_builder->AppendValues({1,2,3});
	//builder_map["MyStructNode"] = struct_node_map;


	//auto float_node = std::make_shared<Node<float, arrow::FloatBuilder>>("MyFloatNode");
	//float_node->createBuilder();
	//auto double_node = std::make_shared<Node<double, arrow::DoubleBuilder>>("MyDoubleNode");
	//double_node->createBuilder();
	//auto list_node = std::make_shared<Node<std::vector<int32_t>, arrow::Int32Builder>>("MyIntListNode");
	//list_node->createBuilder();
	//auto bad_node = std::make_shared<Node<bool, arrow::BooleanBuilder>>("MyBoolNode");
	//bad_node->createBuilder();

	std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> field_map;//{

	return builder_map;

}

//void fill_struct(std::shared_ptr<arrow::ArrayBuilder> builder,
//        std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> struct_field_map
//) {
//
//void fill_struct(std::map<std::string, std::map<std::string, arrow::ArrayBuilder*>> builder_map) {
//
//union data_buffer_t {
//	int32_t int_val;
//	float float_val;
//	std::vector<int > 
//};
//
//	std::map<std::string, data_buffer_t> int_node_buffer;
//	int_node_buffer["MyIntNode"] = 1;
//
//	std::map<std::string, data_buffer_t> struct_node_buffer;
//	struct_node_buffer["bar"] = 1;
//
//	//
//	// go by columns
//	//
//	
//
//    //check_result(struct_builder->Append());
//    //check_result(struct_field_map.at("foo")->Append(1));
//    //check_result(struct_field_map.at("bar")->Append(42.3));
//    //check_result(struct_field_map.at("baz")->AppendValues({7,9,11}));
//    //for(size_t ichild = 0; ichild < n_children; ichild++) {
//    //    std::cout << "child[" << ichild << "] = " << struct_builder->child_builder(ichild)->type()->name() << std::endl;
//
//    //} // ichild
//
//}

//std::shared_ptr<arrow::Table> generate_table3() {
void generate_table3() {

    //auto [struct_builder, struct_field_map] = create_struct_builder();
	auto builder_map = create_struct_builder();
	size_t n_columns = builder_map.size();
	std::cout << "Total number of columns in schema: " << n_columns << std::endl;
	size_t col_num = 0;
	for(const auto& [col_name, col_builder_map] : builder_map) {
		std::cout << "Column [" << (col_num+1) << "/" << n_columns << "]: name = " << col_name << std::endl;
		for(const auto& [var_name, builder] : col_builder_map) {
			std::cout << "			-> var " << var_name << std::endl;
		}
	}
	//fill_struct(builder_map);
    ////fill_struct(struct_builder, struct_field_map);
    std::vector<arrow::Array> arrays;

}

void write_parquet_file(const arrow::Table& table, std::string outname) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(outname));
  auto writer_props = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
  auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table,
                            arrow::default_memory_pool(),
                            outfile,
                            4,
                            writer_props,
                            arrow_props
      )
  );

}

int main(int argc, char* argv[]) {
    //std::shared_ptr<arrow::Table> table = generate_table();
    //write_parquet_file(*table, "struct1.parquet");

    //std::shared_ptr<arrow::Table> table2 = generate_table2();
    //write_parquet_file(*table2, "struct2.parquet");
    generate_table3();
    return 0;
}
